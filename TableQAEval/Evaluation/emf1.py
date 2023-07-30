import re, string, collections

def normalize_answer(s):
    """Lower text and remove punctuation, articles and extra whitespace."""

    def remove_articles(text):
        regex = re.compile(r"\b(a|an|the)\b", re.UNICODE)
        return re.sub(regex, " ", text)

    def white_space_fix(text):
        return " ".join(text.split())

    def remove_punc(text):
        exclude = set(string.punctuation)
        return "".join(ch for ch in text if ch not in exclude)

    def lower(text):
        return text.lower()

    return white_space_fix(remove_articles(remove_punc(lower(s))))


def get_tokens(s):
    if not s:
        return []
    return normalize_answer(s).split()


def compute_exact(a_gold, a_pred):
    return int(normalize_answer(a_gold) == normalize_answer(a_pred))


def compute_f1(a_gold, a_pred):
    gold_toks = get_tokens(a_gold)
    pred_toks = get_tokens(a_pred)
    common = collections.Counter(gold_toks) & collections.Counter(pred_toks)
    num_same = sum(common.values())
    if len(gold_toks) == 0 or len(pred_toks) == 0:
        # If either is no-answer, then F1 is 1 if they agree, 0 otherwise
        return int(gold_toks == pred_toks)
    if num_same == 0:
        return 0
    precision = 1.0 * num_same / len(pred_toks)
    recall = 1.0 * num_same / len(gold_toks)
    f1 = (2 * precision * recall) / (precision + recall)
    return f1


from decimal import Decimal

def compare_percentage_and_decimal(str_percent, str_decimal, tolerance=0.01):
    # 去除空格和逗号
    str_percent = str_percent.replace(" ", "").replace(",", "")
    str_decimal = str_decimal.replace(" ", "").replace(",", "")
    
    # 将百分数转换为小数
    decimal_percent = Decimal(str_percent.strip("%")) / Decimal(100)
    
    # 将小数字符串转换为 Decimal 类型
    decimal_decimal = Decimal(str_decimal)
    
    # 计算两个数之间的差异
    diff = abs(decimal_percent - decimal_decimal)
    
    # 判断差异是否在容忍范围内
    return diff <= Decimal(tolerance)


def compute_emf1(predictions, references):

    # half_correct =0
    # for prediction, ground_truths in zip(predictions, references):
    #     res = metric_max_over_ground_truths(exact_match_score, prediction, ground_truths)
    #     exact_match += res
    #     if res == 1:
    #         correct +=1
    #     if res == 0.25:
    #         half_correct +=1
    # print(f"There are {correct} correct answers \n {half_correct} can not select all correct options\n Total: {len(predictions)} questions.")
    
    
    total = len(references)
    em_list = []
    f1_list = []
    for prediction, reference in zip(predictions, references):
        if '%' in str(prediction):
            em = int(compare_percentage_and_decimal(str(prediction), str(reference)))
        else:
            em = compute_exact(str(prediction), str(reference))
        f1 = compute_f1(str(prediction), str(reference))
        em_list.append(em)
        f1_list.append(f1)

    return sum(em_list) / total, sum(f1_list) / total