import lxml.etree
import markupsafe
import xml.sax.saxutils
from flask.helpers import url_for

class FancyPage(object):
    '''Extended Flask-FlatPages page class.
    
    Provides ability to retrieve headings from the generated page HTML.
    '''
    
    def __init__(self, page):
        self.page = page
    
    # this covers .html
    def __getattr__(self, attr):
        return getattr(self.page, attr)
    
    # this covers .title and other attributes
    def __getitem__(self, key):
        return self.page[key]
    
    @property
    def sections(self):
        doc = lxml.etree.HTML(self.page.html)
        sections = []
        for heading in doc.xpath('//h2'):
            if 'id' in heading.attrib:
                sections.append((heading.attrib['id'], heading.text))
        return sections

class PageIndex(object):
    '''Index of Flask-FlatPages pages.
    
    The primary purpose of this class is to check that the pages that are
    being referenced in intra-site links actually exist.
    '''
    
    def __init__(self, pages):
        self.pages = pages
    
    def url(self, name):
        found = False
        for page in self.pages:
            if name == page.path:
                found = True
                break
        if not found:
            raise ValueError('Page %s not found' % name)
        href = url_for('page', path=page.path)
        title = page.meta['title']
        return markupsafe.Markup('<a href=%s>%s</a>') % (markupsafe.Markup(xml.sax.saxutils.quoteattr(href)), title)
