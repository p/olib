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
    def __repr__(self):
        if self.method:
            method = '#' + self.method
        else:
            method = ''
        return '<TestMeta (%s)' % (self.fullname)

class TestMetaFull(TestMeta):
    def __init__(self, name):
        self.fullname = name
        if '#' in name:
            self.filespec, self.method = name.split('#')
        else:
            self.filespec, self.method = name, None
        self.group, self.lang, self.file = self.filespec.split('.')

class TestMetaFile(TestMeta):
    def __init__(self, name):
        self.lang, self.file = name.split('.')
    
    @property
    def fullname(self):
        raise ValueError, 'No group in meta from file'
    
    @property
    def filespec(self):
        raise ValueError, 'No group in meta from file'
    
    @property
    def group(self):
        raise ValueError, 'No group in meta from file'

class TestDepResolver(object):
    def resolve(self, all_tests, requested_tests):
        tests_to_run = []
        
        needed_tests = {}
        processed = {}
        pending = {}
        for test in requested_tests:
            pending[test] = True
        
        while pending:
            active = pending.keys()[0]
            del pending[active]
            processed[active] = True
            for group, names in all_tests:
                for planned_test in names:
                    checkpoint = '%s.%s' % (group, planned_test.name)
                    if checkpoint == active.filespec:
                        deps = planned_test.dependencies
                        for dep in deps:
                            if dep not in processed and dep not in pending:
                                assert dep.method
                                pending[dep] = True
        
        self.requested_tests = []
        for group, names in all_tests:
            for planned_test in names:
                test_name = '%s.%s' % (group, planned_test.name)
                for comp in processed:
                    if comp.lang == planned_test.meta.lang and comp.file == planned_test.meta.file:
                        tests_to_run.append(group + '.' + planned_test.name)
        
        return tests_to_run
