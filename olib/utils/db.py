def pivot(rows, column):
    map = {}
    for row in rows:
        map[row[column]] = row
    return map

def pivot_multi(rows, *columns):
    map = {}
    for row in rows:
        this_map = map
        for column in columns[:-1]:
            value = row[column]
            if not this_map.has_key(value):
                this_map[value] = {}
            this_map = this_map[value]
        this_map[row[columns[-1]]] = row
    return map

def pivot_lists(rows, column):
    map = {}
    for row in rows:
        key = row[column]
        mapped_row = map.get(key)
        if mapped_row is None:
            mapped_row = []
            map[key] = mapped_row
        mapped_row.append(row)
    return map

def split_row(row, *prefixes):
    if len(prefixes) == 0:
        return [row]
    prefixes = [prefix + '_' for prefix in prefixes]
    base = {}
    parts = []
    r = range(len(prefixes))
    for i in r:
        parts.append({})
    for key in row:
        found = False
        value = row[key]
        for i in r:
            prefix = prefixes[i]
            if key.startswith(prefix):
                parts[i][key[len(prefix):]] = value
                found = True
                break
        if not found:
            base[key] = value
    return [base] + parts

class PropertyDict(object):
    def __init__(self, attrs):
        self.attrs = attrs
    
    def __getattr__(self, attr):
        try:
            return self.attrs[attr]
        except KeyError:
            raise AttributeError, 'No such attribute: %s' % attr
    
    def __repr__(self):
        return 'PropertyDict(%s)' % repr(self.attrs)
    
    def dict(self):
        return self.attrs

def munge_row(row, *prefixes):
    parts = split_row(row, *prefixes)
    base = parts.pop(0)
    for i in range(len(prefixes)):
        base[prefixes[i]] = PropertyDict(parts[i])
    return PropertyDict(base)
