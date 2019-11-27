"""Transform an XML-string to a set of records. Hierarchical records are not treated well enough at the moment,
if a serious source with hierarchical data arrives, """
from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
#import xml.etree.ElementTree as etree
from  lxml import etree
from collections import Counter
from indroid.input.general import Row, Table, Source

class Row(Row):
    def __init__(self, element, tagnames=[]):
        super(Row, self).__init__()
        # Get the sub-elements, text and attributes and put them in a row.
        # First, get the text-value proper:
        # Then get all children/grandchildren etc:
        self.get_child_values(element, tagnames=tagnames)
    def get_child_values(self, element, level=[], tagnames=[], separator='.'):
        if element.text and element.text.strip():
            # The top-level element is not mentioned as key, except if top level has content, then mention element tag.
            key = separator.join(level) if level else element.tag
            self[key] = element.text.strip()
        for child in element:
            if child.tag in tagnames:
                self.setdefault(separator.join(level + [child.tag]), []).append(Row(child, tagnames))
            else:
                self.get_child_values(child, level + [child.tag], tagnames)
        for key in element.keys():
            self[separator.join(level + [key])] = element.get(key)

class Table(Table):
    def __init__(self, tagname, tree, tagnames=[]):
        self.tree = tree
        self.name = self.tagname = tagname
        self.tagnames = tagnames
        self._fieldnames = None
    def __iter__(self):
        def elems_by_tag(elem, tagname, result=[]):
            if elem.tag == tagname:
                result.append(elem)
            for child in elem:
                elems_by_tag(child, tagname, result)
        elems = []
        elems_by_tag(self.tree.getroot(), self.tagname, elems)
        for elem in elems:
            yield Row(elem, self.tagnames)
    @property
    def fieldnames(self):
        if self._fieldnames is None:
            self._fieldnames = []
            fieldnames_set = set()
            for row in self:
                for key in row:
                    if key not in fieldnames_set:
                        self._fieldnames.append(key)
                        fieldnames_set.add(key)
        return self._fieldnames
    @fieldnames.setter
    def fieldnames(self, value):
        self._fieldnames = value

class Source(Source):
    def __init__(self, file_or_filename=None, xml=None, tagnames=[]):
        "Initialize the source based on a file or an xml-string. If both are given, the string is parsed."
        if xml:
            tree = etree.fromstring(xml)
            name = tree.getroot().tag
        elif file_or_filename:
            if isinstance(file_or_filename, basestring):
                self.path, name = os.path.split(file_or_filename)
            elif isinstance(file_or_filename, file):
                self.path, name = os.path.split(file_or_filename.name)
            tree = etree.parse(file_or_filename)
        super(Source, self).__init__(name)
        # Find the first repetitive element if not given:
        if not tagnames:
            tagnames = self.get_tagnames(tree.getroot())
        self.tagnames = tagnames
        for tagname in self.tagnames:
            self._tables.append(Table(tagname, tree, self.tagnames))
    def get_tagnames(self, elem):
        def get_multiple_children(elem1, accu=Counter()):
            result = Counter()
            for child in elem1:
                result[child.tag] += 1
            for tag, count in result.items():
                if count > accu[tag]:
                    accu[tag] = count
            for child in elem1:
                get_multiple_children(child, accu)
            return accu
        tag_count = get_multiple_children(elem)
        return [tag for tag in tag_count if tag_count[tag] > 1]

def test():
    from bizold.input.file import StringWriter
    loc = '\org'
    loc = ''
    for filename in (r"P:\data\source\kpn\eol_zo365\exact{loc}\2014-07-01\2014-07_stambestand.xml",
                     r"P:\data\source\kpn\eol_zo365\exact{loc}\2014-07-01\2014-07_factuurbestand.xml",
                     r"X:\data\source\plato\test\xml{loc}\ftr 2012-01-17.xml",
                     r"X:\data\source\plato\test\xml{loc}\2012-10_factuurbestand.xml",
                     r"X:\data\source\plato\test\xml{loc}\2012-10_stambestand.xml",
                     r"X:\data\source\plato\test\xml{loc}\SMS_SOL_special_mrt 2011-03-16.xml",
                     r"X:\data\source\plato\test\xml{loc}\KPN-2012.07.30_nobackup.xml",
                     r"X:\data\source\plato\test\xml{loc}\KPN-2012.07.25.xml",):
        print os.path.split(filename)[-1]
        src = Source(filename.format(loc=loc))
        for table in src:
            print table.name, table.fieldnames
            f_out = StringWriter(r"X:\data\source\plato\test\xml{loc}\export\{name}.csv".format(loc=loc, name=table.name), fieldnames=table.fieldnames)
            for row in table:
                try:
                    f_out.writerow(row)
                except Exception as e:
                    print e
            f_out.close()

if __name__ == '__main__':
    test()