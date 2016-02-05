import lzma
import wikipedia
from multiprocessing import Pool
import simplejson as json
from lxml import etree

PREFIX = "Wikipedia: "
titles = set()
batch_size = 30


def get_page(title):
    try:
        return wikipedia.page(title, preload=True)
    except:
        pass


if __name__ == '__main__':
    pool = Pool(batch_size)

    f = lzma.LZMAFile('enwiki-latest-abstract.xml.xz')
    fout = lzma.LZMAFile('plain.json.xz', mode='w', options={'level': 9, 'extreme': True})

    parser = etree.iterparse(f, tag="title")

    last_loop = False
    while not last_loop:
        chunk = []
        try:
            for i in xrange(batch_size):
                _, el = parser.next()
                title = el.text
                if title.startswith(PREFIX):
                    title = title[len(PREFIX):]
                    chunk.append(title)
        except StopIteration:
            last_loop = True

        try:
            pages = pool.map(get_page, chunk)
            for page in pages:
                if page and page.title not in titles:
                    print len(titles), page.title
                    titles.add(page.title)
                    d = json.dumps([page.title, page.summary, page.content])
                    fout.write(d + '\n')
        except:
            pass

    fout.close()
    f.close()
