"""
Light-weight implementation to create and write html files. The head tag can
only contain a title and a style sheet, the body tag's content is unlimited.

To create a simple document all you need are the HtmlDocument class, the
HtmlElement class, the add_style method and the add() method:

   doc = HtmlDocument(title='test')
   doc.add_style('.emphasized', 'font-size: 20pt', 'color: green')
   doc.add(HtmlElement(doc, tag='p', class_name='emphasized'))
   doc.print_html(open('test.html'))

This doesn't do anything interesting. The add() method returns the element
added, to which you can add other elements, including texts using HtmlText:

   p = doc.add(HtmlElement(doc, tag='p', class_name='emphasized'))
   p.add(HtmlText('emphasized text'))

Typically, you will use one of the convenience functions on HtmlDocument:

   add_style
   add_paragraph
   add_empty
   add_header
   add_list
   add_numbered_list
   add_text
   add_link
   add_table

All these methods add elements sequentially to the body tag, the only excdption
is the add_styles() method, which always adds to a styes list on the head
element. The add_table() is special in the sense that it returns an HtmlTable
object which itself understands the add_row() method.

A few more examples:

   doc.add_header(None, 'An h2 tag without a style')
   doc.add_text('<p>raw text with <strong>tags</strong> in it</p>')
   doc.add_paragraph('large', 'A p tag with a class')
   doc.add_paragraph(None, 'A p tag without a class')
   doc.add_empty('hr')
   doc.add_link('http://127.0.0.1', 'link to localhost')
   doc.add_list(['a', 'ul', 'list', 'with', 'six', 'items'])
   table = doc.add_table(class_name='indent', padding=8)
   table.add_row(('&nbsp;',), ('term',), ('score',), ('documents',))
   table.add_row(('right', '1'), ('computer program',), ('0.67',), ('right', '16'))
   table.add_row(('right', '2'), ('computer system',), ('0.83',), ('right', '12'))


SOME THINGS ON THE WISHLIST:
- allow writing arbitrary stuff to the head element
- use a StyleSheet object
- add a comment method and object
- some of the convenience methods do not have  attribute support

"""


class HtmlElement(object):
    """This class, and its add() method, can take care of pretty much anything
    in the body tag. Its subclasses however can define more specialized methods
    that, while resulting in the same output, make it more easy to get there."""

    def __init__(self, parent, tag=None, class_name=None, attrs=None):
        self.parent = parent
        self.tag = tag
        self.class_name = class_name
        self.attrs = {} if attrs is None else attrs
        self.children = []

    def add(self, element):
        element.parent = self
        self.children.append(element)
        return element

    def class_string(self):
        return "" if self.class_name is None else " class='%s'" % self.class_name

    def print_html(self, fh, indent=""):
        if self.tag is not None:
            attrs = ""
            if self.attrs:
                attrs = ' ' + ' '.join(["%s='%s'" % (a,v) for a, v in self.attrs.items()])
            fh.write("%s<%s%s%s>\n" % (indent, self.tag, self.class_string(), attrs))
        for element in self.children:
            element.print_html(fh, indent + "   ")
        if self.tag is not None:
            fh.write("%s</%s>\n" % (indent, self.tag))



class HtmlDocument(HtmlElement):

    """Stores the html document and allows incremental changes to it."""

    def __init__(self, title=None):
        """Initialize with a file handle and an optional title."""
        self.title = title
        self.styles = []
        self.children = []

    def add_style(self, name, *specs):
        self.styles.append([name, specs])

    def add_header(self, class_name, text):
        """Create an h2 tag with class and text content and add it to the
        body element."""
        self.children.append(HtmlSimpleElement('h2', class_name, text))

    def add_empty(self, tag):
        self.children.append(HtmlEmptyElement(tag))

    def add_paragraph(self, class_name, text):
        """Create a p tag with class and text content and add it to the body element."""
        self.children.append(HtmlSimpleElement('p', class_name, text))

    def add_table(self, padding=5, spacing=0, border=1, class_name=None):
        """Initialize a table with cellpadding, cellspacing, border and class
        attributes, add it to the body element and return the table so client
        code can add to the table."""
        table = HtmlTable(padding, spacing, border, class_name)
        self.children.append(table)
        return table

    def add_list(self, items):
        ul = doc.add(HtmlElement(doc, 'ul'))
        for item in items:
            ul.add(HtmlSimpleElement('li', None, item))

    def add_numbered_list(self, items):
        ul = doc.add(HtmlElement(doc, 'ul'))
        for item in items:
            ul.add(HtmlSimpleElement('li', None, item))

    def add_text(self, text):
        """Add text or raw html code to the body element."""
        self.children.append(HtmlText(text))

    def add_link(self, url, text):
        """Add an <a> tag with href and text."""
        self.children.append(HtmlLink(url, text))

    def print_html(self, fh, indent=''):
        """Print the html document to the file handle."""
        fh.write("<html>\n\n<head>\n")
        title = fh.name if self.title is None else self.title
        fh.write("<title>%s</title>\n" % title)
        self._print_styles()
        fh.write("</head>\n\n<body>\n")
        for element in self.children:
            element.print_html(fh)
        fh.write("</body>\n\n</html>\n")

    def _print_styles(self):
        """Print the style sheet to the file handle."""
        # should probably have an HtmlStyleSheet class for this
        if self.styles:
            fh.write("<style>\n")
            for style in self.styles:
                fh.write("%s {\n" % style[0])
                for spec in style[1]:
                    fh.write("  %s;\n" % spec)
                fh.write("}\n")
            fh.write("</style>\n")


class HtmlSimpleElement(HtmlElement):
    def __init__(self, tagname, class_name, text):
        self.tag = tagname
        self.class_name = class_name
        self.attrs = {}
        self.children = [HtmlText(text)]

class HtmlEmptyElement(HtmlElement):
    def __init__(self, tag):
        self.tag = tag
        self.class_name = None
        self.attrs = {}
    def print_html(self, fh, indent=''):
        fh.write("%s<%s/>\n" % (indent, self.tag))

class HtmlLink(HtmlElement):
    def __init__(self, url, text):
        self.url = url
        self.text = text
    def print_html(self, fh, indent=''):
        fh.write("%s<a href='%s'>%s</a>\n" % (indent, self.url, self.text))


class HtmlText(HtmlElement):
    """An HtmlText element just contains some text. But note that this text is
    not the same as cdata because it can contain arbitrary text including all
    kinds of html tags. It allows you to put in any kind of html code and you
    would for example split opening and closing tags in two call two this method
    and put other stuff inbetween."""
    
    def __init__(self, text):
        self.text = text

    def print_html(self, fh, indent=''):
        fh.write("%s%s\n" % (indent, self.text))



class HtmlTable(HtmlElement):

    def __init__(self, padding, spacing, border, class_name):
        self.tag = 'table'
        self.class_name = class_name
        self.padding = padding
        self.spacing = spacing
        self.border = border
        self.attrs = { 'cellspacing': spacing, 'cellpadding': padding, 'border': border }
        self.children = []
        
    def add_row(self, *args):
        row = []
        tr = HtmlElement(self, tag='tr')
        for arg in args:
            (align, text) = ('left', arg[0]) if len(arg) == 1 else arg
            td = HtmlElement(tr, tag='td', attrs={'align': align})
            td.add(HtmlText(text))
            tr.add(td)
        self.children.append(tr)



if __name__ == '__main__':

    import sys
    filename = "test.html" if len(sys.argv) == 1 else sys.argv[1]
    fh = open(filename, 'w')
    doc = HtmlDocument('testing')

    doc.add_style('.large', 'font-size: 20pt', 'color: green')
    doc.add_style('.red', 'color: red')
    doc.add_style('.indent', 'margin-left: 20pt')
    doc.add_header(None, "An h2 tag without a style")
    doc.add_text('<p>raw text with <strong>tags</strong> in it</p>')
    doc.add_paragraph('large', "A p tag with a class")
    doc.add_paragraph(None, "A p tag without a class")
    doc.add_empty('hr')
    doc.add_link("http://127.0.0.1", "link to local host")
    doc.add_paragraph(None, "time for a list")
    doc.add_list(['a', 'ul', 'list', 'with', 'six', 'items'])

    doc.add_paragraph(None, "time for a table")
    table = doc.add_table(class_name='indent', padding=8)
    table.add_row(('&nbsp;',), ('term',), ('score',), ('documents',), ('instances',))
    table.add_row(('right', '1'), ('computer program',), ('0.67',), ('right', '16'), ('right', '324'))
    table.add_row(('right', '2'), ('computer system',), ('0.83',), ('right', '12'), ('right', '215'))

    doc.print_html(fh)
