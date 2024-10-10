import xml.sax


class LazyObjectHandler(xml.sax.ContentHandler):
    def __init__(self, callback):
        self.callback = callback
        self.breadcrumb = [{}]
        self.skipped_first_node = False
        self.content = None

    def startElement(self, name, attrs):
        if not self.skipped_first_node:
            self.skipped_first_node = True
            return

        tag = {"name": name, "attrs": attrs, "content": ""}
        head = self.breadcrumb[-1]

        if name in head:
            if type(head[name]) is not list:
                head[name] = [head[name]]
            head[name].append(tag)
        else:
            head[name] = tag

        self.breadcrumb.append(tag)
        self.content = []

    def endElement(self, name):
        self.breadcrumb[-1]["content"] = "".join(self.content)

        self.breadcrumb.pop()

        if len(self.breadcrumb) == 1:
            self.callback(self.breadcrumb[-1][name])
            self.breadcrumb = [{}]

    def characters(self, content):
        if content is None or len(content) == 0 or self.content is None:
            return

        self.content.append(content)


def load_xml(file, callback):
    parser = xml.sax.make_parser()
    parser.setContentHandler(LazyObjectHandler(callback))
    parser.parse(file)
