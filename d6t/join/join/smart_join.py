import pandas as pd
import numpy as np
from collections import OrderedDict
import itertools
import jellyfish

def apply_gen_candidates_group(dfg):
    return pd.DataFrame(list(itertools.product(dfg['__top1left__'].values[0],dfg['__top1right__'].values[0])),columns=['__top1left__','__top1right__'])


def apply_gen_candidates(set1, set2):
    df_candidates = list(itertools.product(set1, set2))
    df_candidates = pd.DataFrame(df_candidates,columns=['__top1left__','__top1right__'])

    return df_candidates


def diff_arithmetic(x,y):
    return abs(x - y)


def diff_edit(a,b):
    return jellyfish.levenshtein_distance(a,b)


def filter_group_minmax(dfg, col):
    """

    Returns all rows equal to min in col

    """
    return dfg[dfg[col] == dfg[col].min()]


def prep_match_df(dfg):
    dfg = dfg[['__top1left__', '__top1right__', '__top1diff__', '__match type__']]
    return dfg


class SmartJoin(object):

    def __init__(self, dfs, keys, mode='exact', how='inner', cfg_top1 = {}, init_merge=False):

        """

        Smart joiner for complex joins

        Args:
            mode (str, list): global string or list for each join. Possible values: ['top1 left','top1 right','top1 bidir all','top1 bidir unmatched']

        """

        if len(dfs)<2:
            raise ValueError('Need to pass at least 2 dataframes')

        if len(dfs)>2:
            raise NotImplementedError('Only handles 2 dataframes for now')

        if not keys or len(keys)<1:
            raise ValueError("Need to have join keys")

        if isinstance(keys[0], (str,)):
            keysdf = [keys]*len(dfs)
            keys = list(map(list, zip(*keysdf)))
        if isinstance(keys[0], (list,)):
            keysdf = list(map(list, zip(*keys)))
        else:
            raise ValueError("keys need to be either list of lists or list of strings")

        self.cfg_njoins = len(keysdf[0])

        if not isinstance(how, (str,)):
            raise NotImplementedError('how can only be applied globally for now')
        elif how in ('right','outer'):
            raise NotImplementedError()
        elif how not in ('left','right','inner','outer'):
            raise ValueError("Invalid how parameter, check documentation for valid values")

        if not all([len(k)==len(dfs) for k in keys]):
            raise ValueError("Need to provide join keys for all dataframes")

        for idf,dfg in enumerate(dfs):
            dfg.head(1)[keysdf[idf]] # check that keys present in dataframe

        if isinstance(mode, (list,)):
            if len(mode)!=self.cfg_njoins:
                raise ValueError("'mode' list needs to have an entry for every join key")
        elif isinstance(mode, (str,)):
            mode = [mode] * self.cfg_njoins

        if not all([m in ('exact','top1','asof') for m in mode]):
            raise ValueError("Invalid mode parameter, check documentation for valid values")
        else:
            if any([m in ('asof',) for m in mode]):
                raise NotImplementedError('asof join not supported yet, use pd.merge_asof() yourself')

        # todo: check cfg_top1.keys correct
        # todo: no duplicate join keys passed
        # todo: check that mode=='left' with top1


        self.dfs = dfs

        # join keys
        self.keys = keys
        self.keysdf = keysdf
        self.keysall = keys+[['__all__']*2]
        self.keysdfall = keysdf+[['__all__']]*2
        self.keyssets_indiv = []
        self.keyssets_merge = []

        def gen_keys(imode):
            return [{'ilevel':ilevel,'how':how[ilevel],'key_left':keysdf[0][ilevel],'key_right':keysdf[1][ilevel]} for ilevel in range(self.cfg_njoins) if mode[ilevel]==imode ]

        self.keysdf_top1 = gen_keys('top1')
        self.keysdf_exact = gen_keys('exact')

        self.mode = mode
        self.how = how
        self.cfg_top1 = cfg_top1

        if init_merge:
            self.join()
        else:
            self.dfjoined = None

        self.table_top1 = {}

    def _calc_keysets(self):

        for idf, dfg in enumerate(self.dfs):
            df_keys = OrderedDict()
            for key in self.keysdf[idf]:
                v = dfg[key].unique()
                df_keys[key] = set(v[~pd.isnull(v)])

            dft = dfg[self.keysdf[idf]].drop_duplicates()
            df_keys['__all__'] = {tuple(x) for x in dft.values}
            self.keyssets_indiv.append(df_keys)

        for keys in self.keysall:
            df_key = {}
            df_key['key left'] = keys[0]
            df_key['key right'] = keys[1]
            df_key['keyset left'] = self.keyssets_indiv[0][df_key['key left']]
            df_key['keyset right'] = self.keyssets_indiv[1][df_key['key right']]

            df_key['inner'] = df_key['keyset left'].intersection(df_key['keyset right'])
            df_key['outer'] = df_key['keyset left'].union(df_key['keyset right'])
            df_key['unmatched total'] = df_key['keyset left'].symmetric_difference(df_key['keyset right'])
            df_key['unmatched left'] = df_key['keyset left'].difference(df_key['keyset right'])
            df_key['unmatched right'] = df_key['keyset right'].difference(df_key['keyset left'])

            vl = next(iter(df_key['keyset left']))
            vr = next(iter(df_key['keyset right']))

            if not type(vl)==type(vr):
                raise ValueError('keys need to be of same type to join', df_key['keyset left'], df_key['keyset right'])

            df_key['value type'] = type(vl)

            self.keyssets_merge.append(df_key)


    def stats_prejoin(self, do_print=True):

        if len(self.keyssets_indiv) == 0:
            self._calc_keysets()

        df_out = []

        for key_set in self.keyssets_merge:
            df_key = {}
            for k in ['keyset left','keyset right','inner','outer','unmatched total','unmatched left','unmatched right']:
                df_key[k] = len(key_set[k])
            for k in ['key left','key right']:
                df_key[k] = key_set[k]
            df_key['all matched'] = df_key['inner']==df_key['outer']
            df_out.append(df_key)

        df_out = pd.DataFrame(df_out)
        df_out = df_out.rename(columns={'keyset left':'left','keyset right':'right'})
        df_out = df_out[['key left','key right','all matched','inner','left','right','outer','unmatched total','unmatched left','unmatched right']]

        if do_print:
            print(df_out)

        return df_out


    def head_input(self, keys_only=True, nrows=3):
        for idf,dfg in enumerate(self.dfs):
            if keys_only:
                print(dfg[self.keysdf[idf]].head(nrows))
            else:
                print(dfg.head(nrows))


    def head_unmatched(self, side='both', level=0, nrecords=3, nrows=3, keys_only=True):

        if side=='both':
                print('unmatched for key ')
                keys = self.keyssets_merge[level]['unmatched both'][:nrecords]
                dfout = []
                for idf, dfg in enumerate(self.dfs):
                    dfg[dfg[self.keysdf[idf]]]
                    raise NotImplementedError()
                    # todo: show nrows for nrecords in self.keyssets_merge[level]['unmatched both'][:nrecords]
        elif side=='left':
            raise NotImplementedError()
        elif side=='right':
            raise NotImplementedError()
        else:
            raise ValueError('invalid side parameter')

        raise NotImplementedError()
        # todo: implement


    def run_match_top1_all(self, cfg_top1=None):

        if cfg_top1:
            self.cfg_top1 = cfg_top1

        for d in self.keysdf_top1:
            self.table_top1[d['key_left']] = self.run_match_top1(d['key_left'])

    def run_match_top1(self, key):

        if key not in self.keysdf[0]:
            raise ValueError('key not found in join keys')

        ilevel = [d['ilevel'] for d in self.keysdf_top1 if d['key_left']==key][0]

        if key not in self.cfg_top1:
            cfg_top1 = {}
        else:
            cfg_top1 = self.cfg_top1[key]

        # make defaults if no settings provided
        if 'fun_diff' not in cfg_top1:

            if len(self.keyssets_merge) == 0:
                self._calc_keysets()
            keyssets = self.keyssets_merge[ilevel]

            if keyssets['value type'] == np.datetime64 or keyssets['value type'] == int:
                cfg_top1['fun_diff'] = diff_arithmetic
            elif keyssets['value type'] == str:
                cfg_top1['fun_diff'] = diff_edit
            else:
                raise ValueError('Unrecognized data type for top match, need to pass fun_diff in arguments')
        else:
            is_valid = callable(cfg_top1['fun_diff']) or (type(cfg_top1['fun_diff']) == list and all([callable(f) for f in cfg_top1['fun_diff']]))
            if not is_valid:
                raise ValueError("'fun_diff' needs to be a function or a list of functions")

        if not type(cfg_top1['fun_diff']) == list:
            cfg_top1['fun_diff'] = [cfg_top1['fun_diff']]


        if 'top_limit' not in cfg_top1:
            cfg_top1['top_limit'] = None

        if 'top_records' not in cfg_top1:
            cfg_top1['top_records'] = None

        return self._gen_match_top1(key, cfg_top1)


    def _gen_match_top1(self, key, cfg_top1):
        """

        Generates match table between two sets

        Args:
            keyssets (dict): values for join keys ['key left', 'key right', 'keyset left', 'keyset right', 'inner', 'outer', 'unmatched total', 'unmatched left', 'unmatched right']
            mode (str, list): global string or list for each join. Possible values: ['exact inner', 'exact left', 'exact right', 'exact outer', 'top1 left', 'top1 right', 'top1 bidir all', 'top1 bidir unmatched']
            is_lower_better (bool): True = difference, False = Similarity

        """

        ilevel = [d['ilevel'] for d in self.keysdf_top1 if d['key_left']==key][0]
        mode = self.mode[ilevel]
        fun_diff = cfg_top1['fun_diff']
        top_limit = cfg_top1['top_limit']
        top_records = cfg_top1['top_records']

        if not mode=='top1':
            raise ValueError('Merge for %s not a top 1 join' %(key))

        #******************************************
        # table LEFT
        #******************************************
        if self.how=='left':

            cfg_group_left = [k for i,k in enumerate(self.keysdf[0]) if self.mode[i]=='exact']
            cfg_group_right = [k for i,k in enumerate(self.keysdf[1]) if self.mode[i]=='exact']
            cfg_value = self.keysdf[0][ilevel]

            if len(cfg_group_left)>0:
                # generate candidates if exact matches are present

                if top_records is None:
                    df_keys_left = pd.DataFrame(self.dfs[0].groupby(cfg_group_left)[cfg_value].unique())
                else:
                    df_keys_left = pd.DataFrame(self.dfs[0].groupby(cfg_group_left)[cfg_value].unique()[:top_records])
                df_keys_right = pd.DataFrame(self.dfs[1].groupby(cfg_group_right)[cfg_value].unique())
                df_keysets_groups = df_keys_left.merge(df_keys_right,left_index=True, right_index=True)
                df_keysets_groups.columns = ['__top1left__','__top1right__']
                dfg = df_keysets_groups.reset_index().groupby(cfg_group_left).apply(apply_gen_candidates_group)
                dfg = dfg.reset_index(-1,drop=True).reset_index()
                dfg = dfg.dropna()

            else:
                # generate candidates if NO exact matches

                if len(self.keyssets_merge) == 0:
                    self._calc_keysets()

                keyssets = self.keyssets_merge[ilevel]

                if top_records is None:
                    dfg = apply_gen_candidates(keyssets['keyset left'],keyssets['keyset right'])
                else:
                    dfg = apply_gen_candidates(list(keyssets['unmatched left'])[:top_records], keyssets['keyset right'])

            for fun_diff in cfg_top1['fun_diff']:
                dfg['__top1diff__'] = dfg.apply(lambda x: fun_diff(x['__top1left__'], x['__top1right__']), axis=1)

                # filtering
                if not top_limit is None:
                    dfg = dfg[dfg['__top1diff__'] <= top_limit]

                # get top 1
                dfg = dfg.groupby('__top1left__',group_keys=False).apply(lambda x: filter_group_minmax(x,'__top1diff__'))

            # return results
            dfg['__match type__'] = 'top1 left'
            df_match = dfg.copy()
            # df_match = prep_match_df(dfg.copy())

        #******************************************
        # table RIGHT
        #******************************************
        elif self.how=='right' or self.how == 'inner':
            raise NotImplementedError('Only use left join for now')
        else:
            raise ValueError("wrong 'how' parameter for top1 join, check documentation")

        return {'key left':self.keysdf[0][ilevel], 'key right':self.keysdf[0][ilevel],
                'table':df_match,'has duplicates':df_match.groupby('__top1left__').size().max()>1}

    def join(self, is_keep_debug=False):
        if all([m=='exact' for m in self.mode]):
            self.dfjoined = self.dfs[0].merge(self.dfs[1], left_on=self.keysdf[0], right_on=self.keysdf[1], how=self.how)
        elif len(self.keysdf_top1)>0:

            if not self.table_top1:
                self.run_match_top1_all()

            self.dfjoined = self.dfs[0]
            for ikey in self.keysdf_top1:
                dft = self.table_top1[ikey['key_left']]['table'].copy()
                dft.columns = [s + ikey['key_left'] for s in dft.columns]
                self.dfjoined = self.dfjoined.merge(dft, left_on=ikey['key_left'], right_on='__top1left__'+ikey['key_left'])

            cfg_keys_left = [d['key_left'] for d in self.keysdf_exact]+['__top1right__'+d['key_left'] for d in self.keysdf_top1]
            cfg_keys_right = [d['key_right'] for d in self.keysdf_exact]+[d['key_right'] for d in self.keysdf_top1]

            self.dfjoined = self.dfjoined.merge(self.dfs[1], left_on = cfg_keys_left, right_on = cfg_keys_right, suffixes=['','__right__'])

            if not is_keep_debug:
                self.dfjoined = self.dfjoined[self.dfjoined.columns[~self.dfjoined.columns.str.startswith('__')]]

        else:
            raise ValueError('No valid mode found, check input')
        return self.dfjoined


