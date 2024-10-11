import re
import csv
import hashlib
import pandas as pd
from pymongo import UpdateOne
from datetime import datetime
from functools import partial
from src.infra.connections_mongodb import MongoDBJobDB
from src.parse.xml_loader import load_xml


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
    content_minus_refs = re.sub("<ref>.*?</ref>", "", content)

    # Look for text matching [[target]] or [[target|display text]]
    pattern = re.compile("\\[\\[([^\\]]+)\\]\\]")

    # # Get positions of links along with the references
    matches = [
        (x.start(), x.group(1).split("|")[0].split("#")[0])
        for x in pattern.finditer(content_minus_refs)
    ]
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
    **kwargs,
):
    element_mapper = build_dict_to_page_mapper()
    
    # batched processing
    batch_size = kwargs.get("batch_size", 100)
    batch_update = []

    def on_element(dto):
        page = element_mapper(dto)
        if page is None:
            return

        # pass each page to the handlers
        [fn(page) for fn in page_handlers]

        # write the title to the CSV file
        if isinstance(page, ContentPage) and node_writer is not None:
            node_writer.writerow([page.title])

        # if the page is a ContentPage, write its links to the CSV file
        if isinstance(page, ContentPage) and edge_writer is not None:
            for ref, pos in page.references:
                edge_writer.writerow([page.title, ref.title, pos])
        
        # if the page is a ContentPage, insert it into MongoDB
        if isinstance(page, ContentPage) and mongodb_client is not None:
            for ref, pos in page.references:
                batch_update.append(
                    UpdateOne(
                        {"title": page.title},
                        {   
                            "$setOnInsert": {
                                "title": page.title,
                                "type": "Page",
                            },
                            "$addToSet": {
                                "references": {
                                    "title": ref.title,
                                    "position": pos,
                                }
                            }
                        },
                        upsert=True,
                    )
                )
                batch_update.append(
                    UpdateOne(
                        {"title": ref.title},
                        {
                            "$setOnInsert": {
                                "title": ref.title,
                                "type": ref.namespace or "Page",
                            },
                        },
                        upsert=True,
                    )
                )
            if len(batch_update) >= batch_size:
                mongodb_client.bulk_write("pages", batch_update)
                batch_update.clear()

    load_xml(file, on_element)
