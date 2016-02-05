import lzma
import wikipedia
from multiprocessing import Pool
import simplejson as json
from lxml import etree
from cPickle import dump, load
import os.path

PREFIX = "Wikipedia: "
titles = set()
batch_size = 20


def get_page(title):
    try:
        return wikipedia.page(title, preload=True)
    except Exception as e:
        print e.__class__, title


if __name__ == '__main__':
    pool = Pool(batch_size)

    if os.path.exists("chunks.dump"):
        with open("chunks.dump", "rb") as f:
            titles = set(load(f))

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
            pages = pool.map(get_page, filter(lambda c: c not in titles, chunk))
            for page in pages:
                if page and page.title not in titles:
                    print len(titles), page.title
                    titles.add(page.title)
                    d = json.dumps([page.title, page.summary, page.content])
                    fout.write(d + '\n')
        except:
            pass
        titles.update(chunk)
        with open("chunks.dump", "wb") as f:
            dump(chunk, f, protocol=2)

    fout.close()
    f.close()
