import lxml.etree

nofollow_offenders = [
    'stackoverflow.com',
    'wikipedia.org',
    'trackpedia.com',
]

# for faster lookups
nofollow_offenders = ['.' + offender for offender in nofollow_offenders]

def nofollow_antiabuse(html):
    import urlparse
    
    doc = lxml.etree.HTML(html)
    
    for element in doc.iter('a'):
        if 'href' in element.attrib:
            href = element.attrib['href']
            parts = urlparse.urlparse(href)
            # assume all offenders run on standard ports.
            # if a non-standard port is used, match will fail
            netloc = parts[1]
            if '.' + netloc in nofollow_offenders:
                if 'rel' in element.attrib:
                    # XXX todo
                    raise NotImplemented
                else:
                    element.attrib['rel'] = 'nofollow'
    
    orig_html, html = html, lxml.etree.tostring(doc)
    
    # seems that lxml is buggy when it comes to serialization
    if '<!DOCTYPE' in orig_html.upper() and '<!DOCTYPE' not in html.upper():
        import re
        match = re.compile(r'<!DOCTYPE[^>]+>', re.I).search(orig_html)
        if match:
            doctype = match.group(0)
            html = doctype + "\n" + html
        xmlns = 'xmlns="http://www.w3.org/1999/xhtml"'
        html = html.replace('%s %s' % (xmlns, xmlns), xmlns)
        xmllang = 'xml:lang="en"'
        html = html.replace('%s %s' % (xmllang, xmllang), xmllang)
    # lxml also destroys all? whitespace between tags
    # in html head, but this is does not break the document so let it slide
    
    return html
