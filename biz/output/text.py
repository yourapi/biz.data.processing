"""General objects for generating data from a source."""
import random
from collections import Counter
from biz.general.general import cached_property, memoized

class Line(object):
    "A container for a line from any source. Used to collect information about separators, fieldnames etc."
    def __init__(self, line):
        self._line = line
    @property
    def line(self):
        return self._line
    @property
    def char_frequencies(self):
        frequencies = Counter()
        for c in self._line:
            frequencies[c] += 1
        return frequencies
    def __repr__(self):
        return self._line
    __str__ = __repr__

class Header(object):
    def __init__(self, source, name=None, fieldnames=[]):
        "Store some metadata about the table"
        self._name = name
        self._fieldnames = fieldnames

class Lines(list):
    def finalize(self):
        "Convert all strings to Line-objects if not already executed."
        for i in range(len(self)):
            if not isinstance(self[i], Line):
                self[i] = Line(self[i])
    @cached_property
    def char_frequencies(self):
        "Return the frequencies of the frequencies of the characters, so the distribution of the characters."
        frequencies = Counter()
        for line in self:
            for char, count in line.char_frequencies.items():
                frequencies.setdefault(char, Counter())[count] += 1
        return frequencies

class Source(object):
    def __init__(self, name=None, date=None):
        self._name = name
        self._date = date
    def __iter__(self):
        raise NotImplementedError("Descendant of Source must implement __iter__ method.")
    @memoized
    def lines(self, max_size=10000, sample=True, first_rows=.1):
        """Read the contents of this source. Since sources can have very large 
        content, get an optional maximum number of lines.
        max_size: the maximum number of lines which is read
        sample: get a sample of the lines. 
        first_rows: the proportion of the sample which consists of the first lines of the source.
                    If this number is > 1, the number is taken absolutely.
        The sample is read in a one-pass process. The sample is not 100% pure, but adequate enough."""
        result = Lines()
        offset = int(first_rows * max_size if first_rows < 1 else first_rows)
        pointer = offset
        for i, line in enumerate(self):
            if i < max_size:
                result.append(line)
            elif not sample:
                break
            else:
                random.seed(line)
                if random.random() < float(max_size) / i:
                    result[pointer] = line
                    pointer =random.randint(offset, max_size - 1)
        result.finalize()
        return result

class File(Source):
    def __init__(self, filename, date=None):
        super(File, self).__init__(filename, date)
        self._filename = filename
    def __iter__(self):
        for line in open(self._filename):
            yield(line)

if __name__ == '__main__':
    f = File(r"E:\Gerard\Dropbox\data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump.csv")
    lines = f.lines()
    print lines[0].char_frequencies
    for char in sorted(lines.char_frequencies.keys()):
        print char
        for num, count in lines.char_frequencies[char].items():
            print num, count, ' ',
        print
        print '-'*80
    open(r"E:\Gerard\Dropbox\data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump-extract.csv", 'w').writelines(l.line for l in lines)