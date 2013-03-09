import sys


class HtmlDocument(object):

    def __init__(self, fh, title=None):
        self.fh = fh
        self.title = title
        self.styles = []
        self.body = []
        
    def add_style(self, name, *specs):
        self.styles.append([name, specs])

    def add_h2(self, class_name, text):
        self.body.append(HtmlSimpleElement('h2', self.fh, class_name, text))

    def add_paragraph(self, class_name, text):
        self.body.append(HtmlSimpleElement('p', self.fh, class_name, text))

    def add_table(self, padding=5, spacing=0, border=1, class_name=None):
        table = HtmlTable(padding, spacing, border, self.fh, class_name)
        self.body.append(table)
        return table

    def add_raw(self, text):
        self.body.append(HtmlRawData(self.fh, text))

    def print_html(self):
        self.fh.write("<html>\n")
        self.fh.write("<head>\n")
        if self.title is not None:
            self.fh.write("<title>%s</title>\n" % self.title)
        self.print_styles()
        self.fh.write("</head>\n")
        self.fh.write("<body>\n")
        for element in self.body:
            element.print_html()
        self.fh.write("</body>\n")
        self.fh.write("</html>\n")

    def print_styles(self):
        # should probably have an HtmlStyleSheet class for this
        if self.styles:
            self.fh.write("<style>\n")
            for style in self.styles:
                self.fh.write("%s {\n" % style[0])
                for a, v in style[1]:
                    self.fh.write("  %s: %s;\n" % (a, v))
                self.fh.write("}\n")
            self.fh.write("</style>\n")


            
class HtmlElement(object):
    
    def class_string(self):
        return "" if self.class_name is None else " class='%s'" % self.class_name

            
class HtmlSimpleElement(HtmlElement):

    def __init__(self, tagname, fh, class_name, text):
        self.tag = tagname
        self.fh = fh
        self.class_name = class_name
        self.text = text

    def print_html(self):
        self.fh.write("<%s%s>%s</%s>\n" % (self.tag, self.class_string(), self.text, self.tag))


class HtmlRawData(HtmlElement):
    """This class allows you to add any kind of html code that you want. This is
    needed because together the other classes are too limited in scope. With
    this class you can, for example, split the opening and closing tags and
    print some other stuff inbetween."""
    
    def __init__(self, fh, text):
        self.fh = fh
        self.text = text

    def print_html(self):
        self.fh.write("%s\n" % self.text)


class HtmlTable(HtmlElement):

    def __init__(self, padding, spacing, border, fh, class_name):
        self.fh = fh
        self.class_name = class_name
        self.padding = padding
        self.spacing = spacing
        self.border = border
        self.rows = []
        
    def add_row(self, *args):
        row = []
        for arg in args:
            (align, text) = ('left', arg[0]) if len(arg) == 1 else arg
            row.append((align, text))
        self.rows.append(row)

    def print_html(self):
        self.fh.write("<table cellpadding=%d cellspacing=%d border=%d%s>\n" % \
                     (self.padding, self.spacing, self.border, self.class_string()))
        for row in self.rows:
            self.fh.write("<tr>\n")
            for (align, text) in row:
                self.fh.write("  <td align=%s>%s\n" % (align, text))
            self.fh.write("</tr>\n")
        self.fh.write("</table>\n")




if __name__ == '__main__':

    fh = open('test.html', 'w')
    doc = HtmlDocument(fh, 'testing')
    doc.add_style('.large', ('font-size', '20pt'), ('color', 'red'))
    doc.add_style('.indent', ('margin-left', '20pt'))
    doc.add_h2(None, "This is a header")
    doc.add_paragraph('large', "How is it going?")
    doc.add_paragraph('indent', "I am fine")
    table = doc.add_table(class_name='indent')
    table.add_row(('&nbsp;',), ('term',), ('score',), ('documents',), ('instances',))
    table.add_row(('right', '1'), ('computer program',), ('0.67',), ('right', '16'), ('right', '324'))
    doc.print_html()
    
    
