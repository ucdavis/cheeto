
import pytest
from rich import print
from ruamel.yaml.parser import ParserError

from .. import yaml


class TestParseYaml:

    def test_zero_prefixed_int(self, testdata):
        '''
        Test that zero-prefixed ints do not parse as strings.
        '''
        fn = testdata('testgroup.yaml')
        parsed = yaml.parse_yaml(fn)
        assert 'group' in parsed
        assert parsed['group']['testgroup']['gid'] == 111111

    def test_empty(self, testdata):
        '''
        Test that an empty YAML returns {}
        '''
        fn = testdata('testempty.yaml')
        parsed = yaml.parse_yaml(fn)
        assert parsed == {}

    def test_malformed(self, testdata):
        '''
        Test that a malformed YAML raises.
        '''
        fn = testdata('testmalformed.yaml')
        with pytest.raises(ParserError):
            parsed = yaml.parse_yaml(fn)

    def test_nonexistent(self):
        '''
        Make sure FileNotFoundError is caught and returns {}
        '''
        fn = 'not-a-file'
        parsed = yaml.parse_yaml(fn)
        assert parsed == {}


class TestPuppetMerge:

    def test_precedence(self):
        '''
        Test that objects later in the parameter list take precedence.
        '''
        a = {'a': 1}
        b = {'a': 2}
        merged = yaml.puppet_merge(a, b)
        assert merged['a'] == 2

    def test_lists(self):
        '''
        Test that lists are properly concatenated.
        '''
        a = {'a': [1, 2, 3]}
        b = {'a': [1, 2, 3]}
        merged = yaml.puppet_merge(a, b)
        assert merged['a'] == [1, 2, 3, 1, 2, 3]

    def test_sets(self):
        '''
        Test that sets are properly unioned.
        '''
        a = {'a': {1, 2, 3}}
        b = {'a': {1, 2, 3}}
        merged = yaml.puppet_merge(a, b)
        assert merged['a'] == {1, 2, 3}

    def test_type_precedence(self):
        '''
        Test that the type of the override takes precedence.
        '''
        a = {'a': [1, 2, 3]}
        b = {'a': 1}
        merged = yaml.puppet_merge(a, b)
        assert merged['a'] == 1

    def test_deep_merge(self):
        '''
        Test a multi-level merge.
        '''
        a = {'a': [1, 2, 3],
             'b': {'aa': 1,
                   'bb': [1, 2, 3]
                   }
             }
        b = {'a': 1,
             'b': {'aa': 2,
                   'bb': [4]
                   }
             }
        merged = yaml.puppet_merge(a, b)
        assert merged['b']['aa'] == 2
        assert merged['b']['bb'] == [1, 2, 3, 4]


class TestParseYamlForest:

    def test_merge_all(self, testdata):
        f1_fn, f1_site_fn, f2_fn, f2_site_fn = testdata('test-forest-1.yaml',
                                                        'test-forest-1.site.yaml',
                                                        'test-forest-2.yaml',
                                                        'test-forest-2.site.yaml')
        forest = yaml.parse_yaml_forest([f1_fn, f1_site_fn, f2_fn, f2_site_fn],
                                        merge_on = yaml.MergeStrategy.ALL)
        assert 'merged-all' in forest
        assert len(forest) == 1
        
        data = forest['merged-all']
        assert data['a'] == 3
        assert data['b'] == 'cheeto'


    def test_merge_none(self, testdata):
        f1_fn, f1_site_fn, f2_fn, f2_site_fn = testdata('test-forest-1.yaml',
                                                        'test-forest-1.site.yaml',
                                                        'test-forest-2.yaml',
                                                        'test-forest-2.site.yaml')
        forest = yaml.parse_yaml_forest([f1_fn, f1_site_fn, f2_fn, f2_site_fn],
                                        merge_on = yaml.MergeStrategy.NONE)
        assert len(forest) == 4
        
        assert forest[f1_fn]['a'] == 1
        assert forest[f1_site_fn]['a'] == 2

        assert 'b' not in forest[f2_fn]
        assert forest[f2_fn]['a'] == 3

        assert 'a' not in forest[f2_site_fn]
        assert forest[f2_site_fn]['b'] == 'cheeto'

    def test_merge_prefix(self, testdata):
        f1_fn, f1_site_fn, f2_fn, f2_site_fn = testdata('test-forest-1.yaml',
                                                        'test-forest-1.site.yaml',
                                                        'test-forest-2.yaml',
                                                        'test-forest-2.site.yaml')
        forest = yaml.parse_yaml_forest([f1_fn, f1_site_fn, f2_fn, f2_site_fn],
                                        merge_on = yaml.MergeStrategy.PREFIX)

        assert len(forest) == 2
        assert 'test-forest-1' in forest
        assert 'test-forest-2' in forest

        forest_1 = forest['test-forest-1']
        assert forest_1['a'] == 2
        assert forest_1['b'] == [1, 2, 3]

        forest_2 = forest['test-forest-2']
        assert forest_2['a'] == 3
        assert forest_2['b'] == 'cheeto'
