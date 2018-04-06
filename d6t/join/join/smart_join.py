import pandas as pd
import numpy as np
from collections import OrderedDict
import itertools
import jellyfish

def diff_candidates(set1, set2, fun_diff):
    """

    Calculates difference/similarity for all possible candidate pairs in two sets

    Args:
        set1 (list): values set 1
        set2 (list): values set 2
        fun_diff (function): difference/similarity function to apply

    """
    df_candidates = list(itertools.product(set1, set2))
    df_candidates = pd.DataFrame(df_candidates,columns=['__left__','__right__'])

    df_candidates['__diff__']=df_candidates.apply(fun_diff, axis=1)

    return df_candidates


def diff_arithmetic(row):
    return abs(row['__left__'] - row['__right__'])


def diff_edit(row):
    return jellyfish.levenshtein_distance(row['__left__'],row['__right__'])


def filter_group_minmax(dfg, col, min=True):
    """

    Returns all rows equal to min or max in col

    """
    if min:
        return dfg[dfg[col] == dfg[col].min()]
    else:
        return dfg[dfg[col] == dfg[col].max()]

def prep_match_df(dfg):
    dfg = dfg[['__left__', '__right__', '__diff__', '__match type__']]
    return dfg

def prep_match_df2(dfg, key_left, key_right):
    # todo: remove diff and match type
    # add keys to left and right column names
    return dfg



    
class SmartJoin(object):

    def __init__(self, dfs, keys, mode='exact inner', init_merge=False):

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

        if not isinstance(keys[0], (list,)):
            keysdf = [keys]*len(dfs)
            keys = list(map(list, zip(*keysdf)))
        else:
            keysdf = list(map(list, zip(*keys)))

        if not all([len(k)==len(dfs) for k in keys]):
            raise ValueError("Need to provide join keys for all dataframes")

        for idf,dfg in enumerate(dfs):
            dfg[keysdf[idf]] # check that keys present in dataframe

        if isinstance(mode, (list,)):
            if len(mode)!=len(keys):
                raise ValueError("'mode' list length needs to consistent with number of join keys")
        elif isinstance(mode, (str,)):
            mode = [mode] * (len(dfs) - 1)

        self.dfs = dfs
        self.keys = keys
        self.keysdf = keysdf
        self.keysall = keys+[['__all__']*2]
        self.keysdfall = keysdf+[['__all__']]*2
        self.mode = mode
        self.keyssets_indiv = []
        self.keyssets_merge = []

        if init_merge:
            self.join()
        else:
            self.dfjoined = None


    def _calc_keysets(self):

        for idf,dfg in enumerate(self.dfs):
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


    def join(self):
        if all([m=='exact' for m in self.mode]):
            self.dfjoined = self.dfs[0].merge(self.dfs[1], left_on=self.keysdf[0], right_on=self.keysdf[1], how=self.how[0])
            # todo: what if I want to how to change for each key instead of global parameter?
        else:
            raise NotImplementedError()


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


    def gen_match_tables(self, ilevel, is_lower_better=True, fun_diff=None, top_limit = None, top_records = None):
        """

        Generates match table between two sets

        Args:
            keyssets (dict): values for join keys ['key left', 'key right', 'keyset left', 'keyset right', 'inner', 'outer', 'unmatched total', 'unmatched left', 'unmatched right']
            mode (str, list): global string or list for each join. Possible values: ['exact inner', 'exact left', 'exact right', 'exact outer', 'top1 left', 'top1 right', 'top1 bidir all', 'top1 bidir unmatched']
            is_lower_better (bool): True = difference, False = Similarity

        """

        if len(self.keyssets_indiv) == 0:
            self._calc_keysets()

        keyssets = self.keyssets_merge[ilevel]
        mode = self.mode[ilevel]

        df_match = pd.DataFrame()

        if not mode=='top1 bidir unmatched':
            dfg = pd.DataFrame(list(keyssets['inner']),columns=['__left__'])
            dfg['__right__'] = dfg['__left__']
            dfg['__diff__'] = np.nan
            dfg['__match type__'] = 'exact inner'
            df_match_inner = df_match.append(dfg)
        else:
            df_match_inner = None

        if fun_diff is None and 'top1' in mode:
            if keyssets['value type'] == np.datetime64 or keyssets['value type'] == int:
                fun_diff = diff_arithmetic
            elif keyssets['value type'] == str:
                fun_diff = diff_edit
            else:
                raise ValueError('Unrecognized data type for top match, need to pass fun_diff in arguments')

        #******************************************
        # table LEFT
        #******************************************
        if 'top1' in mode and ('left' in mode or 'bidir' in mode):
            if top_records is None:
                dfg = diff_candidates(keyssets['unmatched left'],keyssets['keyset right'], fun_diff)
            else:
                dfg = diff_candidates(list(keyssets['unmatched left'])[:top_records], keyssets['keyset right'], fun_diff)
            dfg['__match type__'] = 'top1 left'

            # get top 1
            if not top_limit is None:
                if is_lower_better:
                    dfg = dfg[dfg['__diff__']<=top_limit]
                else:
                    dfg = dfg[dfg['__diff__']>=top_limit]

            dfg = dfg.groupby('__left__',group_keys=False).apply(lambda x: filter_group_minmax(x,'__diff__',is_lower_better))

            # return results
            df_match_left = prep_match_df(dfg.copy())

        elif mode=='exact left' or mode=='exact outer':
            dfg = pd.DataFrame(list(keyssets['unmatched left']),columns=['__left__'])
            dfg['__left__'] = np.nan
            dfg['__match type__'] = 'exact left'
            df_match_left = prep_match_df(dfg.copy())

        else:
            df_match_left = None

        #******************************************
        # table RIGHT
        #******************************************
        if 'top1' in mode and ('right' in mode or 'bidir' in mode):
            if top_records is None:
                dfg = diff_candidates(keyssets['keyset left'], keyssets['unmatched right'], fun_diff)
            else:
                dfg = diff_candidates(keyssets['keyset left'], list(keyssets['unmatched right'])[:top_records], fun_diff)

            dfg['__match type__'] = 'top1 right'

            # get top 1
            if not top_limit is None:
                if is_lower_better:
                    dfg = dfg[dfg['__diff__']<=top_limit]
                else:
                    dfg = dfg[dfg['__diff__']>=top_limit]
            dfg = dfg.groupby('__right__',group_keys=False).apply(lambda x: filter_group_minmax(x,'__diff__',is_lower_better))

            # return results
            df_match_right = prep_match_df(dfg.copy())

        elif mode=='exact right' or mode=='exact outer':
            dfg = pd.DataFrame(list(keyssets['unmatched right']),columns=['__right__'])
            dfg['__right__'] = np.nan
            dfg['__match type__'] = 'exact right'
            df_match_right = prep_match_df(dfg.copy())

        else:
            df_match_right = None

        return {'key left':keyssets['key left'], 'key right':keyssets['key right'],
                'table inner':df_match_inner, 'table left':df_match_left, 'table right':df_match_right,
                'table left all': prep_match_df(pd.concat([df_match_inner,df_match_left])).sort_values('__left__'), 'table right all': prep_match_df(pd.concat([df_match_inner,df_match_right])).sort_values('__right__'),
                'left has duplicates':df_match_left.groupby('__left__').size().max()>2 if not df_match_left is None else False,
               'right has duplicates':df_match_right.groupby('__right__').size().max()>2 if not df_match_right is None else False}
