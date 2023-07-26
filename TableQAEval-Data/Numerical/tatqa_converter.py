# coding=utf-8
import sys
import argparse
from pathlib import Path
import ast
import re
import json
from ast2json import ast2json


my_parser = argparse.ArgumentParser(description="Convert TAT-QA data file into FinQA format.")
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

    def __init__(self, j):
        self.j = j
        self.headers = self.retrieve_headers()
        self.header_tags = self.tag_headers()
        self.data_begins_at = len(self.headers)
        self.merged_header = self.merge_headers()
        if not self.merged_header:
            self.table = self.j['table'][self.data_begins_at:]
        else:
            self.table = [self.merged_header] + \
                self.j['table'][self.data_begins_at:]
        self.table = self.tokenify(self.table)

    def retrieve_headers(self):
        headers = [self.j['table'][0]]
        non_empties = [len(x.strip()) > 0 for x in self.j['table'][0]]
        non_empties[0] = True
        for row in self.j['table'][1:]:
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
        ls = [len(row) for row in self.j['table']]
        return len(set(ls)) == 1

    def verbalize(self, i, j, toggle=False):
        fix = ''
        if self.table[0][j]:
            fix = ' of ' + self.table[0][j]
        if toggle: cell = self.table[i][j].replace('-', '').strip() 
        else: cell = self.table[i][j] 
        return 'The ' + self.table[i][0] + fix + ' is ' + cell + ' ;'


class Program:
    mapping = {'Add': 'add', 'Sub': 'subtract',
               'Mult': 'multiply', 'Div': 'divide'}

    def __init__(self, derivation, value):
        self.value = value
        # Fix the bugs in the dataset
        derivation = derivation
        if derivation == '-114 - (71)':
            print('Found!')
            derivation = '-114 - (-71)'
        if derivation == '53%*$23,406 ':
            print('Found!')
            derivation = '0.53*$23,406'
        self.derivation = derivation.replace(
            '[', '(').replace(']', ')').strip()
        self.expr = self.clean(self.derivation)
        self.tree = ast.parse(self.expr)
        self.j = ast2json(self.tree)
        assert len(self.j['body']) == 1
        self.j = self.j['body'][0]
        assert self.j['_type'] == 'Expr'
        self.j = self.j['value']
        self.consts = []
        self.finqa_format = self.convert()

    def clean(self, text):
        return text.replace(',', '').replace('%', '').replace('$', '').\
            replace('billions', '').replace('millions', '').replace('thousands', '').\
            replace('billion', '').replace('million', '').replace('thousand', '').\
            strip()

    def convert(self):
        idx, f = self.to_finqa_format(self.j, -1)
        f = f.replace('subtract(#0, 1)', 'subtract(#0, const_1)')
        # sometimes a rescaling is needed
        e = eval(self.expr)
        if e == 0.0:
            return f
        if int(self.value / e) in [99, 100]:
            f += ', multiply(#' + str(idx) + ', const_100)'
        return f

    def to_finqa_format(self, j, idx):
        _type = j['_type']
        assert _type in ['BinOp', 'UnaryOp', 'Constant']
        if _type == 'Constant':
            s = str(j['value'])
            self.consts.append(s)
            # 3.8 and 3.80 are both added to constants
            if '.' in s and len(s.split('.')[1]) == 1:
                self.consts.append(s + '0')
                self.consts.append(s + '00')
            if '.' in s and s[-1] == '0':
                self.consts.append(s[:-1])
            return idx, str(j['value'])
        if _type == 'UnaryOp':
            assert j['op']['_type'] == 'USub'
            operand = j['operand'].copy()
            if 'value' in operand: operand['value'] = -1 * operand['value']
            ix, x = self.to_finqa_format(operand, idx)
            return idx, x
        left = j['left']
        op = j['op']['_type']
        assert op in ['Add', 'Sub', 'Mult', 'Div']
        op = self.mapping[op]
        if _type == 'BinOp':
            right = j['right']
            il, l = self.to_finqa_format(left, idx)
            ir, r = self.to_finqa_format(right, il)
            if il == idx and ir == idx:
                return idx + 1, op + '(' + l + ', ' + r + ')'
            elif il == idx and ir != il:
                irs = '#' + str(ir)
                return ir + 1, r + ', ' + op + '(' + l + ', ' + irs + ')'
            elif ir == il and il != idx:
                ils = '#' + str(il)
                return il + 1, l + ', ' + op + '(' + ils + ', ' + r + ')'
            else:
                ils = '#' + str(il)
                irs = '#' + str(ir)
                return ir + 1, l + ', ' + r + ', ' + op + '(' + ils + ', ' + irs + ')'
        il, l = self.to_finqa_format(left, idx)
        ils = '#' + str(il)
        return il + 1, l + ', ' + op + '(' + ils + ',  NONE)'

    def locate(self, table):
        output = []
        for i, row in enumerate(table.table[1:]):
            for j, cell in enumerate(row):
                c = self.clean(cell)
                c = c.replace('*', '').strip()
                if c in self.consts or c.replace('(', '').replace(')', '').strip() in self.consts:
                    output.append((str(i + 1), table.verbalize(i + 1, j, False)))
                elif c.replace('-', '').strip() in self.consts:
                    output.append((str(i + 1), table.verbalize(i + 1, j, True)))
        return output


def convert_tat_qa_text_to_post_text(item):
    post_text = []
    for paragraph in item['paragraphs']:
        sentences = paragraph['text'].split(".")
        for sentence in sentences:
            tokens = tokenize(sentence)
            if len(tokens) > 0:
                post_text.append(tokens + " .")
    return post_text


def convert_tatqa_to_finqa(item, idx):
    output = {}
    q = item['questions'][idx]
    output['id'] = q['uid']
    output['qa'] = {}
    output['qa']['question'] = q['question']
    output['qa']['exe_ans'] = q['answer']
    output['table_ori'] = item['table']
    table = Table(item['table'])
    output['table'] = table.table
    program = Program(q['derivation'], q['answer'])
    output['qa']['program'] = program.finqa_format
    facts = program.locate(table)
    facts_dict = {}
    for ix, f in facts:
        i = "table_" + str(ix)
        if i not in facts_dict:
            facts_dict[i] = f
        else:
            facts_dict[i] += ' ' + f
    output['qa']['gold_inds'] = facts_dict
    for par_idx in q['rel_paragraphs']:
        p = int(par_idx)
        for par in item['paragraphs']:
            if par['order'] == p:
                output['qa']['gold_inds'][
                    'text_' + str(p - 1)] = tokenize(par['text'])
    output['pre_text'] = []
    output['post_text'] = convert_tat_qa_text_to_post_text(item)
    return output


def run(path: str):
    output = []
    i = 1
    with open(path, 'r') as f:
        j = json.load(f)
        for item in j:
            for idx, q in enumerate(item["questions"]):
                if q["answer_type"] == "arithmetic" and \
                        q['derivation'] not in ['31 January 2019 + 10 years'] and \
                        'average' not in q['question'].lower():
                    output.append(convert_tatqa_to_finqa(item, idx))
                    i += 1
    return output


def stats(train, dev, test):
    cnt_per_step = {1:0, 2:0, 3:0}
    with open(train, 'r') as f:
        j = json.load(f)
        for sample in j:
            prog = sample['qa']['program']
            prog = prog.replace('(', '{').replace(')', '{').replace(',', '{').split('{')
            prog = [x for x in prog if len(x.strip()) > 0]
            l = len(prog)//3
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
            if l < 3: key = l
            else: key = 3
            cnt_per_step[key] = cnt_per_step[key]+1
    with open(test, 'r') as f:
        j = json.load(f)
        for sample in j:
            prog = sample['qa']['program']
            prog = prog.replace('(', '{').replace(')', '{').replace(',', '{').split('{')
            prog = [x for x in prog if len(x.strip()) > 0]
            l = len(prog)//3
            if l < 3: key = l
            else: key = 3
            cnt_per_step[key] = cnt_per_step[key]+1
    for key, value in cnt_per_step.items():
        print(key, value)


if __name__ == "__main__":
    
    args = my_parser.parse_args()
    input_path = Path(args.input_path)

    if not input_path.exists():
        print(f"The given input path does not exist: {input_path}")
        sys.exit(1)

    j = run(args.input_path)
    with open(args.output_path, 'w') as f:
        json.dump(j, f)
