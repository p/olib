import unittest

def test_firstlineno_cmp(a, b):
    a_val = getattr(a, a._testMethodName).__func__.func_code.co_firstlineno
    b_val = getattr(b, b._testMethodName).__func__.func_code.co_firstlineno
    return cmp(a_val, b_val)

class TestLoader(object):
    def __init__(self):
        try:
            # python 2.7
            self.test_loader = unittest.loader.defaultTestLoader
        except AttributeError:
            # python 2.6
            self.test_loader = unittest.defaultTestLoader
    
    def load_test_module(self, name):
        if name.startswith('.'):
            raise ValueError, 'Relative module imports not supported'
        module = __import__(name)
        hier = name.split('.')
        for child in hier[1:]:
            module = getattr(module, child)
        return module
    
    def load_tests_in_module(self, name):
        module = self.load_test_module(name)
        tests = self.test_loader.loadTestsFromModule(module)
        all_tests = []
        for suite in tests:
            suite_tests = [test for test in suite]
            suite_tests.sort(test_firstlineno_cmp)
            all_tests += suite_tests
        return all_tests

class TestMeta(object):
    def __init__(self, name):
        self.fullname = name
        if '#' in name:
            self.filespec, self.method = name.split('#')
        else:
            self.filespec, self.method = name, None
        self.group, self.lang, self.file = self.filespec.split('.')
    
    def __repr__(self):
        if self.method:
            method = '#' + self.method
        else:
            method = ''
        return '<TestMeta (%s)' % (self.fullname)
