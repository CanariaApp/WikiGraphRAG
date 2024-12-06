import re
import hashlib
from pymongo import UpdateOne
from datetime import datetime
from functools import partial
from src.infra.connections_mongodb import MongoDBJobDB
from src.parse.xml_loader import load_xml
from datasets import load_dataset
import numpy as np
from collections import defaultdict
import pandas as pd
import os


# keep a set for unique titles and links


def get_hash(txt: str):
    return hashlib.md5((txt).encode("utf-8")).hexdigest()


class Page:
    def __init__(self):
        self.title = None
        self.namespace = None
        self.id = None
        self.last_edit = None

    def __eq__(self, other):
        return (
            type(other) is Page
            and self.title == other.title
            and self.namespace == other.namespace
            and self.id == other.id
            and self.last_edit == other.last_edit
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class RedirectPage(Page):
    def __init__(self):
        super()
        self.target = None

    def __str__(self):
        return f"{self.namespace}.{self.id} ({self.last_edit}): '{self.title}' -> '{self.target.title}'"

    def __eq__(self, other):
        return (
            type(other) is RedirectPage
            and Page.__eq__(other)
            and self.target == other.target
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class ContentPage(Page):
    def __init__(self):
        super()
        self.content = None
        self.links = []
        self.references = []

    def __str__(self):
        return f"{self.namespace}.{self.id} ({self.last_edit}): '{self.title}' -> '{self.content[0:64]}'"

    def __eq__(self, other):
        return (
            type(other) is ContentPage
            and Page.__eq__(other)
            and self.content == other.content
            and self.references == other.references
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class RedirectPageTarget:
    def __init__(self):
        self.title = None
        self.namespace = None

    def __eq__(self, other):
        return (
            type(other) is RedirectPageTarget
            and self.title == other.title
            and self.namespace == other.namespace
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class PageLocation:
    def __init__(self, title, namespace):
        self.title = title
        self.namespace = namespace

    def __eq__(self, other):
        return (
            type(other) is PageLocation
            and self.title == other.title
            and self.namespace == other.namespace
        )

    def __ne__(self, other):
        return not self.__eq__(other)


def _extract_references(content):
    """
    Extract internal references (links) from the page content.
    Returns a list of tuples: (link_title, position).
    """
    # Remove <ref> elements as they don't count as references in this context.
    #content_minus_refs = re.sub("<ref>.*?</ref>", "", content)

    # Look for text matching [[target]] or [[target|display text]]
    #pattern = re.compile("\\[\\[([^\\]]+)\\]\\]")

    # # Get positions of links along with the references
    #matches = [
    #    (x.start(), x.group(1).split("|")[0].split("#")[0])
    #    for x in pattern.finditer(content_minus_refs)
    #]
    matches = []

    # matches = [(x.start(), "") for x in pattern.finditer(content_minus_refs)]

    # # Use finditer and tuple unpacking for more efficient extraction
    # matches = [(match.start(), match.group(1)) for match in pattern.finditer(content_minus_refs)]

    # Return list of (link, position) tuples
    return matches


def _map_dict_to_page_model(page, parse_page_location_fn):
    if "redirect" in page:
        page_location = parse_page_location_fn(page["redirect"]["attrs"]["title"])

        model = RedirectPage()
        model.target = RedirectPageTarget()
        model.target.title = page_location
        model.target.namespace = page_location.namespace
    else:
        model = ContentPage()
        model.content = page["revision"]["text"]["content"]

        # extract references and positions
        model.references = [
            (parse_page_location_fn(ref), pos)
            for pos, ref in _extract_references(model.content)
            if ref not in ["", f"{parse_page_location_fn(ref).namespace}:"]
        ]

    page_location = parse_page_location_fn(page["title"]["content"])
    model.id = int(page["id"]["content"])
    model.title = page_location.title  # .replace(" ", "_")
    model.namespace = page_location.namespace
    model.last_edit = datetime.fromisoformat(
        page["revision"]["timestamp"]["content"][:-1]
    )

    return model


def _get_page_location(namespace_set, title):
    def capitalize(s):
        if len(s) <= 1:
            return s.upper()
        return s[0].upper() + s[1:]

    # Extract string before the first '|' if present
    sanitized_title = title.split("|")[0]  # Get the string before '|'
    # replace underscores with spaces
    sanitized_title = re.sub("[\\s_]+", "_", sanitized_title).strip()
    # Pages that start with "W:" (e.g. "W:Pants") are the same as pages in the main namespace with the preface
    sanitized_title = re.sub("[\\s_]+", "_", title).strip()

    if title.startswith("W:") or title.startswith("w:"):
        return PageLocation(capitalize(sanitized_title[2:]), None)

    capitalized_title = capitalize(sanitized_title)
    detected_namespace = title.split(":")[0]
    if detected_namespace == "Talk":
        return PageLocation(capitalized_title.split("/")[0], "Talk")

    namespace = (
        detected_namespace
        if detected_namespace in namespace_set and detected_namespace != ""
        else None
    )
    return PageLocation(capitalized_title, namespace)


def build_dict_to_page_mapper():
    parse_page_location_fn = None

    def on_element(dto):
        nonlocal parse_page_location_fn

        if dto["name"] == "siteinfo":
            # The lookup isn't required right now, but it helps during debugging
            namespace_lookup = {
                int(x["attrs"]["key"]): x["content"]
                for x in dto["namespaces"]["namespace"]
            }
            namespace_set = set(namespace_lookup.values())
            parse_page_location_fn = partial(_get_page_location, namespace_set)
            return None

        elif dto["name"] == "page":
            return _map_dict_to_page_model(dto, parse_page_location_fn)

    return on_element

def iterate_pages_from_export_file(
    file,
    page_handlers=[],
    node_writer=None,
    edge_writer=None,
    mongodb_client: MongoDBJobDB=None,
    wiki_redirects=defaultdict(None),
    **kwargs,
):
    element_mapper = build_dict_to_page_mapper()
    
    # batched processing
    batch_size = kwargs.get("batch_size", 100)
    batch_update = []

    class Parquet_iterator:
        def __init__(self, parquet_file_loc):
            #Load the first file
            self.parquet_file_loc = parquet_file_loc
            self.data_iter = iter(pd.read_parquet(self.parquet_file_loc+"/000.parquet").to_dict(orient='records'))
            self.file_ind = 0

        def __iter__(self):
            return self

        def __next__(self):
            res = next(self.data_iter,None)
            if res is None: #EOF
                file_ind_text = str(self.file_ind+1).zfill(3)
                parquet_file_name = self.parquet_file_loc + "/" + file_ind_text + ".parquet"
                if os.path.exists(parquet_file_name):
                    self.file_ind += 1
                    self.data_iter = iter(pd.read_parquet(parquet_file_name).to_dict(orient='records'))
                    res = next(self.data_iter, None)
            return res


    ##### BGE3 from downloaded files
    # Start reading in bge3 dataset for page paragraphs
    bge3_dataset = Parquet_iterator('src/data/embeddings')

    ##### BGE3 streamed from Huggingface
    # #Start streaming in the bge3 dataset for page paragraphs
    # bge3_dataset = iter(load_dataset("Upstash/wikipedia-2024-06-bge-m3", "en", split="train", streaming=True))

    last_loaded_paragraph = next(bge3_dataset)

    def mongo_add_paragraph(rec_id, title, content, embedding ):
        return UpdateOne(
            {"id": rec_id},
            {
                "$setOnInsert": {
                    "id": rec_id,
                    "title": title,
                    "type": "Paragraph",
                    "content": content,
                    "embedding": embedding,
                },
            },
            upsert=True,
        )

    def mongo_add_ref(rec_id, ref_id, ref_title ):
        return UpdateOne(
                {"id": rec_id},
                {
                    "$addToSet": {
                        "references": {
                            "id": ref_id,
                            "title": ref_title,
                        }
                    }
                },
                upsert=True,
            )

    def on_element(dto):
        nonlocal last_loaded_paragraph
        if last_loaded_paragraph is None:  # No more bge3 data
            return

        page = element_mapper(dto)
        if page is None:
            return
        if not isinstance(page, ContentPage):
            return

        #Check if the current page corresponds to the current paragraph title
        curr_title = last_loaded_paragraph["title"]
        #print(page.title,curr_title)
        if (page.title != curr_title) and (page.title.lower() != wiki_redirects.get(curr_title.lower(),"")) and (page.title.lower().replace("_"," ") != curr_title.lower().replace("_"," ")):
            return #Not the page you are looking for, go to next, since they are sorted on title

        # pass each page to the handlers
        [fn(page) for fn in page_handlers]


        # Accumulating paragraphs corresponding to this page title, taking advantage of the fact that the dataset is sorted on it
        bge3_paragraphs = []
        while (last_loaded_paragraph["title"] == curr_title):
            bge3_paragraphs.append(last_loaded_paragraph)
            last_loaded_paragraph = next(bge3_dataset,None)
            if last_loaded_paragraph is None: #No more bge3 data
                break
        #Break up page content into paragraphs, keeping the ones longer than 100 characters
        page_paragraphs = [p for p in page.content.split('\n') if len(p)>10]
        if (len(page_paragraphs) == 0): #not sure if this would ever happen
            page_paragraphs = page.content.split('\n')
        if (len(page_paragraphs)<len(bge3_paragraphs)):
            raise Exception("Fewer page paragraphs than BGE3 paragraphs!")
        #Go over each paragraph and try to match them with bge3
        bge3_ind = 0
        ref_paragraph_word_set = set(bge3_paragraphs[bge3_ind]["text"].replace("\'","").split(' '))
        bge3_newline_count = bge3_paragraphs[bge3_ind]["text"].count("\n")
        bge3_length = len(bge3_paragraphs[bge3_ind]["text"])
        bge3_paragraph_refs = defaultdict(list)
        ref_pattern = re.compile("\\[\\[([^\\]]+)\\]\\]") # References look like [[target]] or [[target|display text]]

        for par_ind,par in enumerate(page_paragraphs):
            if (bge3_newline_count > 0): #Unfortunately the BGE3 dataset has some paragraphs with multiple newlines
                par = '\n'.join(page_paragraphs[par_ind:(min(par_ind+bge3_newline_count, len(page_paragraphs)))])
            if (len(par)<bge3_length/4) and (abs(len(par)-bge3_length)>200): #this paragraph is too short
                continue
            content_minus_refs = re.sub("<ref>.*?</ref>", "", par)
            #content_cleaned = re.sub(r'{{.*?}}',"",re.sub(r'\[\[(?:[^|\]]*\|)?([^]]+)\]\]', r'\1', content_minus_refs).replace("'''","").replace("\'",""))
            content_cleaned = re.sub(r'[*"`\']|({{.*?}})', "", re.sub(r'\[\[(?:[^|\]]*\|)?([^]]+)\]\]', r'\1', content_minus_refs))
            content_word_set =  set(content_cleaned.split(' '))
            # fraction of the reference is in the current text
            score1 = 1.0 - len(ref_paragraph_word_set - content_word_set) / len(ref_paragraph_word_set)
            # fraction of the current text is in the reference
            score2 = 1.0 - len(content_word_set - ref_paragraph_word_set) / len(content_word_set)
            score = np.sqrt(score1*score2)

            # if (score>0.2) and (score<0.9):
            #     print("\n")
            #     print(bge3_paragraphs[bge3_ind]["text"].replace("\'",""))
            #     print("\n")
            #     print(content_cleaned)
            #     print(score, bge3_ind)
            #     print("\n")

            if (score > 0.5):
                refs = [x.split("|")[0].split("#")[0].lower() for x in re.findall(ref_pattern, content_minus_refs)]
                #Apply redirects
                for i,ref in enumerate(refs):
                    if ref in wiki_redirects:
                        refs[i] = wiki_redirects[ref]
                bge3_paragraph_refs[bge3_paragraphs[bge3_ind]["id"]] = refs
                bge3_ind += 1
                if (bge3_ind >= len(bge3_paragraphs)):
                    break  # stop if nothing is left to match
                ref_paragraph_word_set = set(bge3_paragraphs[bge3_ind]["text"].replace("\'","").split(' '))
                bge3_newline_count = bge3_paragraphs[bge3_ind]["text"].count("\n")


        par_title_norm = wiki_redirects.get(bge3_paragraphs[-1]["title"].lower(),bge3_paragraphs[-1]["title"].lower()) #get the current title, replace if it is a redirect
        for ind, par in enumerate(bge3_paragraphs):
            par_id =  par_title_norm+"_"+str(ind)
            # Insert page
            if mongodb_client is not None:
                batch_update.append( mongo_add_paragraph(par_id, par["title"], par["text"], par["embedding"].tolist()) )
            if node_writer is not None:
                node_writer.writerow([par_id, par["title"], par["text"].replace("\n"," ")])
            # Insert reference links within the page
            if ( ind==0 ): #first paragraph
                #Link it to all other paragraphs on the page
                for target_ind in range(1,len(bge3_paragraphs)):
                    target_id = par_title_norm + "_" + str(target_ind)
                    if mongodb_client is not None:
                        batch_update.append( mongo_add_ref(par_id, target_id, par["title"] ) )
                    if edge_writer is not None:
                        edge_writer.writerow([par_id, target_id ])
            elif ( ind < (len(bge3_paragraphs)-1) ):
                #Link it to the next paragraph
                target_id = par_title_norm + "_" + str(ind+1)
                if mongodb_client is not None:
                    batch_update.append(mongo_add_ref(par_id, target_id, par["title"]))
                if edge_writer is not None:
                    edge_writer.writerow([par_id, target_id ])
            for ref in bge3_paragraph_refs[par["id"]]:
                #Insert reference links to other pages
                target_id = ref+"_0"
                if mongodb_client is not None:
                    batch_update.append(mongo_add_ref(par_id, target_id, ref))
                if edge_writer is not None:
                    edge_writer.writerow([par_id, target_id ])

                # #Doing this is not necessary, we can drop links that point nowhere at a later time
                # #Insert page the reference points to
                # batch_update.append( mongo_add_paragraph(ref + "_0", ref, ???, ???]) )

                if (len(batch_update) >= batch_size):
                    mongodb_client.bulk_write("pages", batch_update)
                    batch_update.clear()

    load_xml(file, on_element)


def iterate_pages_to_find_redirects(file, page_handlers=[], **kwargs):
    wiki_redirects = defaultdict(None)
    element_mapper = build_dict_to_page_mapper()

    def on_element(dto):
        page = element_mapper(dto)
        if page is None:
            return

        # pass each page to the handlers
        [fn(page) for fn in page_handlers]

        if isinstance(page, RedirectPage):
            if ( page.title.lower() != page.target.title.title.lower() ):
                wiki_redirects[page.title.lower()] = page.target.title.title.lower()

    load_xml(file, on_element)
    return  wiki_redirects
