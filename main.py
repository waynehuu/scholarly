import scholarly
import pandas as pd
from tqdm import tqdm
import time
import random
import os
import argparse


_YEAR_SINCE = 2015  # Format: YYYY
_YEAR_TO = None  # Format: year_to should be no less than year_since
_RESULT_ITEMS = 10
_SAVE_ROOT = os.path.join(os.getcwd(), 'tasks', 'run04132020')
_RESUME = True
_START_FROM = 10

energy_terms = [
    # 'Wind',
    # 'Solar', # 5 left
    # 'Power system', # Done
    # 'Energy',
    # 'Generator', # Done
    'Coal',
    # 'Oil',
    # 'Natural Gas',
    # 'Geothermal',
    # 'Hydropower',
    # 'Power line',
    # 'Transmission line',
    # 'Electricity line',
    # 'Energy infrastructure',
    # 'Electric infrastructure',
    # 'Renewable' # Done
]

ml_terms = [
    'machine learning',
    'deep learning',
    'support vector machine',
    'random forest',
    'regression tree',
    'neural network',
    'regression',
    'classification',
    'detection'
]

rs_terms = [
    'remote sensing',
    'satellite',
    'aerial',
    'UAV',
    'unmanned aerial vehicle',
    'hyperspectral',
    'imagery',
    'sentinel',
    'landsat',
    'infrared'
]


def quote(s):
    quotation_mark = '\"'
    return quotation_mark + s + quotation_mark


def make_url(kw, year_since, year_to):
    url = '/scholar?&q={}'.format(kw)

    if year_since is not None:
        assert isinstance(year_since, int)
        url += '&as_ylo={}'.format(year_since)

    if year_to is not None:
        assert isinstance(year_to, int)
        if year_since is not None:
            assert year_to >= year_since
        url += '&as_yhi={}'.format(year_to)

    return url + '&start={}'.format(_START_FROM)


def check_existence(kw_checklist, file_checklist, e, m, r, save_name):
    comb = '{} + {} + {}'.format(e, m, r)
    f_comp = open(kw_checklist, 'r')
    f_file = open(file_checklist, 'r')

    for line in f_comp.readlines():
        if comb in line:
            return True

    for line in f_file.readlines():
        if save_name in line:
            return True

    return False


def make_filename(terms, year_since, year_to, n_items):

    if isinstance(terms, list):
        for term in terms:
            term = term.replace(' ', '_')
        fname = '_'.join(terms)
    else:
        fname = terms

    if year_since:
        fname += '_since_{}'.format(year_since)

    if year_to:
        fname += '_to_{}'.format(year_to)

    fname += '_start={}'.format(_START_FROM)

    return '{}_first_{}.csv'.format(fname, n_items)


def actual_scrape(result_items, search_query, e, m, r):
    results = []

    i = 0
    while i < int(result_items):
        try:
            res = next(search_query)
            i += 1
            if hasattr(res, 'citedby'):
                res.bib['citedby'] = res.citedby
            else:
                res.bib['citedby'] = 'NA'
            res.bib['kw1'] = e
            res.bib['kw2'] = m
            res.bib['kw3'] = r
            results.append(res.bib)
        except StopIteration:
            if len(results) == 0:
                return 'Stopped, probably got blocked'
            else:
                return results

    return results


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--start', help='start=? in api query', default=0, type=int)
    args = parser.parse_args()
    _START_FROM = args.start

    for e in energy_terms:

        print('Scraping for energy term: {} '.format(e))

        e_dir = os.path.join(os.path.curdir, _SAVE_ROOT, e)
        if not os.path.exists(e_dir):
            os.makedirs(e_dir)

        completion_file_path = os.path.join(e_dir, 'completion.txt')
        f_comp = open(completion_file_path, 'a+')

        file_list_path = os.path.join(e_dir, 'finished.txt')
        f_file = open(file_list_path, 'a+')

        for m in tqdm(ml_terms):

            for r in rs_terms:

                save_name = make_filename(
                    [m, r], _YEAR_SINCE, _YEAR_TO, _RESULT_ITEMS)

                ifExists = check_existence(
                    completion_file_path, file_list_path, e, m, r, save_name) if _RESUME else False

                if ifExists:
                    continue

                kw = '+'.join([quote(e), quote(m), quote(r)])
                kw = kw.replace(' ', '%20')

                if _YEAR_SINCE or _YEAR_TO:
                    url = make_url(kw, _YEAR_SINCE, _YEAR_TO)
                    search_query = scholarly.search_pubs_custom_url(url)
                else:
                    search_query = scholarly.search_pubs_query(kw)

                results = actual_scrape(_RESULT_ITEMS, search_query, e, m, r)

                if isinstance(results, str):
                    print('\nQuery {} + {} + {}: {}'.format(e, m, r, results))
                    print('Query url: https://scholar.google.com{}'.format(url))
                    return None
                else:
                    # f_comp.write(
                    #     'Query {} + {} + {}: Finished\n'.format(e, m, r))

                    f_file.write(save_name)
                    f_file.write('\n')

                    results_pd = pd.DataFrame.from_dict(results)
                    results_pd.to_csv(os.path.join(
                        e_dir, save_name), index=False)

        f_comp.close()
        f_file.close()

if __name__ == '__main__':
    main()
