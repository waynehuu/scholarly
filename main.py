import scholarly
import pandas as pd
from tqdm import tqdm
import time
import random


year_since = 2015  # Format: YYYY
year_to = None  # Format: year_to should be no less than year_since

result_items = 20

energy_terms = [
    # 'Wind',
    # 'Solar',
    'Power system',
    # 'Energy',
    # 'Generator',
    # 'Coal',
    # 'Oil',
    # 'Natural Gas',
    # 'Geothermal',
    # 'Hydropower',
    # 'Power line',
    # 'Transmission line',
    # 'Electricity line',
    # 'Energy infrastructure',
    # 'Electric infrastructure'
]

ml_terms = [
    'machine learning',
    'deep learning',
    'support vector machine',
    'random forest',
    'regression tree',
    'neural network'
]

rs_terms = [
    'remote sensing',
    'satellite',
    'aerial',
    'UAV',
    'unmanned aerial vehicle',
    'hyperspectral'
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

    return url


def make_filename(energy_term, year_since, year_to, n_items):

    fname = energy_term

    if year_since:
        fname += '_since_{}'.format(year_since)

    if year_to:
        fname += '_to_{}'.format(year_to)

    return '{}_first_{}.csv'.format(fname, n_items)


for e in energy_terms:
    print('Scraping for energy term: {}'.format(e))
    results = []
    for m in tqdm(ml_terms):
        for r in rs_terms:
            kw = '+'.join([quote(e), quote(m), quote(r)])
            kw = kw.replace(' ', '%20')
            if year_since or year_to:
                url = make_url(kw, year_since, year_to)
                search_query = scholarly.search_pubs_custom_url(url)
            else:
                search_query = scholarly.search_pubs_query(kw)
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
                    break
    results_pd = pd.DataFrame.from_dict(results)
    results_pd.to_csv(make_filename(e, year_since, year_to, result_items), index=False)
