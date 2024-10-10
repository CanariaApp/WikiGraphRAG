import re
import csv
import hashlib
import pandas as pd
from datetime import datetime
from functools import partial
from src.parse.xml_loader import load_xml
from src.infra.connections_mysql import MySQLConnector
from src.infra.connections_aerospike import AerospikeConnector


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


# def _extract_references(content):
#     # While the <ref></ref> element can definitely contain references, I exclude them here since my concept of a
#     # reference can be summed up as "a link that I'm likely to see while reading the content body of a page"
#     content_minus_refs = re.sub("<ref>.*?</ref>", "", content)

#     # Look for text matching [[target]] or [[target|display text]]
#     pattern = re.compile("\\[\\[([^\\]]+)\\]\\]")
    
#     return [
#         x.split("|")[0].split("#")[0] for x in re.findall(pattern, content_minus_refs)
#     ]

def _extract_references(content):
    """
    Extract internal references (links) from the page content.
    Returns a list of tuples: (link_title, position).
    """
    # Remove <ref> elements as they don't count as references in this context.
    content_minus_refs = re.sub("<ref>.*?</ref>", "", content)

    # Look for text matching [[target]] or [[target|display text]]
    pattern = re.compile("\\[\\[([^\\]]+)\\]\\]")
    
    # Get positions of links along with the references
    matches = [(x.start(), x.group(1).split("|")[0].split("#")[0]) for x in pattern.finditer(content_minus_refs)]
    
    # Return list of (link, position) tuples
    return matches


def _map_dict_to_page_model(page, parse_page_location_fn):
    if "redirect" in page:
        page_location = parse_page_location_fn(page["redirect"]["attrs"]["title"])

        model = RedirectPage()
        model.target = RedirectPageTarget()
        model.target.title = page_location.title
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
    model.title = page_location.title
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

    # Pages that start with "W:" (e.g. "W:Pants") are the same as pages in the main namespace with the preface
    sanitized_title = re.sub("[\\s_]+", " ", title).strip()
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


def insert_to_mysql(data, mysql_client):
    """Insert data into MySQL using the given MySQLConnector."""
    # convert the data batch into a DataFrame
    df = pd.DataFrame(data, columns=["title", "link", "pos"])
    # replace " " with "_"
    df["title"] = df["title"].str.replace(" ", "_")
    df["link"] = df["link"].str.replace(" ", "_")
    # filter if title and link have more than 511 characters
    df = df[(df["title"].str.len() <= 1023) & (df["link"].str.len() <= 1023)].reset_index(drop=True)
    # # get hash values for the page and link titles
    df["title_link_hash"] = df.apply(
        lambda row: get_hash(f"{row['title']}_{row['link']}"), 
        axis=1,
    )
    df["title_hash"] = df["title"].apply(get_hash)
    df["link_hash"] = df["link"].apply(get_hash)
    # print(df)
    # insert to mysql
    mysql_client.insert_dataframe(
        table_name="wiki_links", 
        df=df, 
        verbose=False,
        primary_keys=["title_link_hash"],
    )


def iterate_pages_from_export_file(
        file, 
        page_handlers=[], 
        node_writer=None,
        edge_writer=None,
        mysql_client: MySQLConnector=None,
        aerospike_client: AerospikeConnector=None,
        **kwargs,
    ):
    element_mapper = build_dict_to_page_mapper()

    batch_size = kwargs.get("batch_size", 10000)
    data_batch = []

    num_threads = kwargs.get("num_threads", 1)

    def on_element(dto):
        page = element_mapper(dto)
        if page is None:
            return

        # pass each page to the handlers
        [fn(page) for fn in page_handlers]

        # insert to mysql
        if isinstance(page, ContentPage):
            for ref, pos in page.references:
                data_batch.append((page.title, ref.title, pos))
        # insert into MySQL in batches
        if len(data_batch) >= batch_size:
            if mysql_client is not None:
                insert_to_mysql(data_batch, mysql_client)
            data_batch.clear()  # Clear the batch after insertion

        # write the title to the CSV file
        if isinstance(page, ContentPage) and node_writer is not None:
            node_writer.writerow([page.title.replace(" ", "_")])

        # if the page is a ContentPage, write its links to the CSV file
        if isinstance(page, ContentPage) and edge_writer is not None:
            for ref, pos in page.references:
                edge_writer.writerow([page.title.replace(" ", "_"), ref.title.replace(" ", "_"), pos])


        # write titles to Aerospike
        if isinstance(page, ContentPage) and aerospike_client is not None:
            # insert the page title as the main key and link titles as values in Aerospike
            page_key = page.title.replace(" ", "_")
            aerospike_client.put(
                namespace="wiki",
                set_name="page_links",
                key=page_key,
                value={
                    "exists": True,
                    "title": page.title,
                    "links": [link.title.replace(" ", "_") for link, _ in page.references],
                },
            )
            # placeholder for referenced pages
            # too slow
            if False:
                for link, _ in page.references:
                    if not aerospike_client.read("wiki", "page_links", key=link.title.replace(" ", "_")):
                        link_key = link.title.replace(" ", "_")
                        aerospike_client.put(
                            namespace="wiki",
                            set_name="page_links",
                            key=link_key,
                            value={
                                "exists": True,
                                "title": link_key,
                            },
                        )

    load_xml(file, on_element)
