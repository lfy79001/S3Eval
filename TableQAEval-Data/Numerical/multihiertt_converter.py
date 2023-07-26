# coding=utf-8
import os
import sys
import argparse
from pathlib import Path
import ast
import re
import json
from ast2json import ast2json
from bs4 import BeautifulSoup


my_parser = argparse.ArgumentParser(description="Convert MultiHiertt data file into FinQA format.")
my_parser.add_argument("--input", "-i", dest="input_path", type=str, help="The path to input file.")
my_parser.add_argument("--output", "-o", dest="output_path", type=str, help="The path to output file.")


def tokenize(txt):
    text = txt.lower().replace('(', '-').replace(')', '').replace(',', '')
    return re.sub('\s+', ' ', ' '.join(re.split(r"([^a-zA-Z0-9\-\,\.])", text))).strip()


class Table:
    period_keywords = ['year', 'period', 'fiscal', 'ended' 'ending', 'months',
                       'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december',
                       '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']
    scale_keywords = ['thousand', 'thousands', 'million', 'millions', 'billion', 'billions',
                      '$', '%', '€', '£', '¥', 'usd', 'gbp', 'rmb',
                      'percentage', 'percent', 'change', 'bps', 'basis points']

    def __init__(self, j, idx):
        self.j = j
        self.raw = {}
        self.raw['table'] = self.get_table(idx)
        self.headers = self.retrieve_headers()
        self.header_tags = self.tag_headers()
        self.data_begins_at = len(self.headers)
        self.merged_header = self.merge_headers()
        if not self.merged_header:
            self.table = self.raw['table'][self.data_begins_at:]
        else:
            self.table = [self.merged_header] + \
                self.raw['table'][self.data_begins_at:]
        self.table = self.tokenify(self.table)

    def get_table(self, idx):
        tree = BeautifulSoup(self.j[int(idx)], "html.parser")
        output = []
        for row in tree.find_all('th'):
            r = []
            for cell in row.find_all('td'):
                r.append(cell.text)
            output.append(r)
        for row in tree.find_all('tr'):
            r = []
            for cell in row.find_all('td'):
                r.append(cell.text)
            output.append(r)
        return output

    def retrieve_headers(self):
        headers = [self.raw['table'][0]]
        non_empties = [len(x.strip()) > 0 for x in self.raw['table'][0]]
        non_empties[0] = True
        for row in self.raw['table'][1:]:
            if all(non_empties) and len(row[0].strip()) > 0:
                break  # continue until all empty columns are covered
            headers.append(row)
            e = [i for i, x in enumerate(row) if x]
            for i in e:
                non_empties[i] = True
        return headers

    def tag_cell(self, cell):
        if not cell:
            return ''
        c = cell.lower().strip()
        for kw in self.period_keywords:
            if kw in c:
                return 'period'
        for kw in self.scale_keywords:
            if kw in c:
                return 'scale'
        return 'metric'

    def tag_headers(self):
        header_tags = []
        for header in self.headers:
            header_tags.append([self.tag_cell(cell) for cell in header])
        return header_tags

    def repeat_headers(self):
        tags = self.header_tags[:]
        for i, row in enumerate(self.header_tags):
            for j, cell in enumerate(row):
                if cell not in ['scale', 'period']:
                    continue
                # copy to previous cell based on previous row
                if i > 0 and j > 0 and self.header_tags[i - 1][j - 1] in ['scale', 'period'] and self.header_tags[i][j - 1] == '':
                    self.headers[i][j - 1] = self.headers[i][j]
                    tags[i][j - 1] = tags[i][j]
                # copy to next cell based on previous row
                if i > 0 and j < len(row) - 1 and self.header_tags[i - 1][j + 1] in ['scale', 'period'] and self.header_tags[i][j + 1] == '':
                    self.headers[i][j + 1] = self.headers[i][j]
                    tags[i][j + 1] = tags[i][j]
                # copy to previous cell based on next row
                if i < len(self.header_tags) - 1 and j > 0 and self.header_tags[i + 1][j - 1] in ['scale', 'period'] and self.header_tags[i][j - 1] == '':
                    self.headers[i][j - 1] = self.headers[i][j]
                    tags[i][j - 1] = tags[i][j]
                # copy to next cell based on next row
                if i < len(self.header_tags) - 1 and j < len(row) - 1 and self.header_tags[i + 1][j + 1] in ['scale', 'period'] and self.header_tags[i][j + 1] == '':
                    self.headers[i][j + 1] = self.headers[i][j]
                    tags[i][j + 1] = tags[i][j]
        self.header_tags = tags
        return True

    def merge_headers(self):
        self.repeat_headers()
        merged_headers = [list(x) for x in zip(*self.headers)]
        merged_tags = [list(x) for x in zip(*self.header_tags)]
        for i in range(len(merged_headers)):
            for j in range(len(merged_headers[i])):
                if merged_tags[i][j] == 'scale' and '(' not in merged_headers[i][j]:
                    merged_headers[i][j] = '(' + merged_headers[i][j] + ')'
        output = []
        for row in merged_headers:
            output.append(' '.join(row).strip())
        return output

    def tokenify(self, table):
        output = []
        for row in table:
            output.append([tokenize(cell) for cell in row])
        return output

    def is_square(self, table):
        ls = [len(row) for row in self.raw['table']]
        return len(set(ls)) == 1

    def verbalize(self, i, j, toggle=False):
        fix = ''
        if self.table[0][j]:
            fix = ' of ' + self.table[0][j]
        if toggle: cell = self.table[i][j].replace('-', '').strip() 
        else: cell = self.table[i][j] 
        return 'The ' + self.table[i][0] + fix + ' is ' + cell + ' ;'


def convert_multihiertt_text_to_post_text(item):
    post_text = []
    for paragraph in item['paragraphs']:
        sentences = paragraph.split(".")
        for sentence in sentences:
            tokens = tokenize(sentence)
            if len(tokens) > 0:
                post_text.append(tokens + " .")
    return post_text


def convert_multihiertt_to_finqa(j, idx):
    output = {}
    output['id'] = j['uid']
    output['qa'] = {}
    output['qa']['question'] = j['qa']['question']
    output['qa']['exe_ans'] = j['qa']['answer']
    output['qa']['program'] = j['qa']['program'].replace(',', ', ')
    table = Table(j['tables'], idx)
    output['table_ori'] = table.raw['table']
    output['table'] = table.table
    facts_dict = {}
    for ix in j['qa']['table_evidence']:
        row = ix.split('-')[1]
        i = "table_" + str(row)
        if i not in facts_dict:
            facts_dict[i] = tokenize(j['table_description'][ix][:-1])
        else: 
            facts_dict[i] += ' ' 
            facts_dict[i] += tokenize(j['table_description'][ix][:-1])
    for ix in j['qa']['text_evidence']:
        i = 'text_' + str(ix)
        facts_dict[i] = tokenize(j['paragraphs'][ix])
    output['qa']['gold_inds'] = facts_dict
    output['pre_text'] = []
    output['post_text'] = convert_multihiertt_text_to_post_text(j)
    return output


def run(path: str):
    output = []
    i = 1
    with open(path, 'r') as f:
        j = json.load(f)
        for sample in j:
            if sample['qa']['question_type'] != 'arithmetic': continue
            table_indices = sample['qa']['table_evidence']
            table_indices = [x.split('-')[0] for x in table_indices]
            if len(set(table_indices)) > 1: continue
            try:
                s = convert_multihiertt_to_finqa(sample, table_indices[0])
                prog = s['qa']['program']
                prog = prog.replace('(', '{').replace(')', '{').replace(',', '{').split('{')
                prog = [x for x in prog if len(x.strip()) > 0]
                l = len(prog)//3
                if l > 0:
                    output.append(s)
                    i += 1
            except IndexError:
                pass
    return output


def stats(train, dev):
    cnt_per_step = {1:0, 2:0, 3:0}
    with open(train, 'r') as f:
        j = json.load(f)
        for sample in j:
            prog = sample['qa']['program']
            prog = prog.replace('(', '{').replace(')', '{').replace(',', '{').split('{')
            prog = [x for x in prog if len(x.strip()) > 0]
            l = len(prog)//3
            if l == 0: print(sample['id'], sample['qa']['program'])
            if l < 3: key = l
            else: key = 3
            cnt_per_step[key] = cnt_per_step[key]+1
    with open(dev, 'r') as f:
        j = json.load(f)
        for sample in j:
            prog = sample['qa']['program']
            prog = prog.replace('(', '{').replace(')', '{').replace(',', '{').split('{')
            prog = [x for x in prog if len(x.strip()) > 0]
            l = len(prog)//3
            if l == 0: print(sample['qa']['program'])
            if l < 3: key = l
            else: key = 3
            cnt_per_step[key] = cnt_per_step[key]+1
    for key, value in cnt_per_step.items():
        print(key, value)


if __name__ == "__main__":

    # stats('../../datasets/multihiertt/multihiertt_dataset_train_finqa.json', '../../datasets/multihiertt/multihiertt_dataset_dev_finqa.json')
    
    args = my_parser.parse_args()
    input_path = Path(args.input_path)

    if not input_path.exists():
        print(f"The given input path does not exist: {input_path}")
        sys.exit(1)

    j = run(args.input_path)
    print(len(j))
    with open(args.output_path, 'w') as f:
        json.dump(j, f)