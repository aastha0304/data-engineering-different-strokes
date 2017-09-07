import urllib
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
import re
import nltk, string

#nltk.download('punkt') # if necessary...

#https://stats.stackexchange.com/questions/301333/how-to-determine-summary-like-tables-on-any-informative-web-html-page
#http://blog.yhat.com/posts/logistic-regression-and-python.html
stemmer = nltk.stem.porter.PorterStemmer()
remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)


def stem_tokens(tokens):
    return [stemmer.stem(item) for item in tokens]


'''remove punctuation, lowercase, stem'''
def normalize(text):
    return stem_tokens(nltk.word_tokenize(text.lower().translate(remove_punctuation_map)))


vectorizer = TfidfVectorizer(tokenizer=normalize)  #, stop_words='spanish')


def cosine_sim(text1, text2):
    tfidf = vectorizer.fit_transform([text1, text2])
    return ((tfidf * tfidf.T).A)[0,1]


def search(values, search_for):
    for k in values:
        if isinstance(values[k], (list,set,tuple)):
            for v in values[k]:
                #print('to find :',search_for,' strs in list ',v)
                if search_for in v:
                    #print('found in one of in list ',search_for, v)
                    return k
        else:
            if search_for in values[k]:
                #print('found in str ',search_for, values[k])
                return k
    return None


def find_depth(table):
    n = 0
    p = table
    while p is not None:
        p = p.parent
        n += 1
    return n


def tfidf(table, soup):
    t_text = table.text
    w_text = soup.text
    return cosine_sim(t_text, w_text)


def find_tr_td(table):
    n_row = 0
    max_col = 0
    for row in table.find_all('tr', recursive=False):
        cols = row.find_all('td', recursive=False)
        max_col = max(max_col, len(cols))
        n_row += 1
    return (n_row, max_col)


def handle_width(attr):
    val = None
    if 'width' in attr:
        val = attr['width']
    else:
        key = search(attr, 'width')
        if key is not None:
            val = attr[key]
    return val


def text_to_tag_ratio(table):
    regex = r'\w+'
    text_num = len(re.findall(regex, table.text))
    tag_num = len([tag.name for tag in table.find_all()])
    if tag_num != 0:
        return float(text_num/tag_num)
    return 0


def has(table, tag):
    return 1 if len(table.find_all(tag)) != 0 else 0


def has_attr(attr, txt, table):
    val = 1 if search(attr, txt) is not None or search(table.attrs, txt) is not None else 0
    if val == 0:
        for children in table.find_all(recursive=False):
            val = 1 if search(children.attrs, txt) is not None else 0
            if val == 0:
                for child in children.find_all(recursive=False):
                    val = 1 if search(child.attrs, txt) is not None else 0
    return val


def build_table_row(e,table,soup):
    attr = table.attrs
    p_attr = {}
    parent = table.parent
    children = parent.find_all(recursive=False)
    if len(children) == 1:
        p_attr = parent.attrs
    idx=e
    is_table = 1
    is_aside = 0
    float_right = 1 if search(attr, 'float: right') is not None or search(p_attr, 'float: right') is not None or \
                       search(attr, 'float:right') is not None or search(p_attr, 'float:right') is not None \
                else 0
    depth_table = find_depth(table)
    tf_idf=tfidf(table,soup)
    no_rows,max_no_col = find_tr_td(table)
    #width=handle_width(attr)
    #if width is None:
        #width = handle_width(p_attr)
    texttag_ratio = text_to_tag_ratio(table)
    has_th = has(table, 'th')
    has_img = has(table, 'img')
    has_infobox = has_attr(p_attr, 'infobox', table) | has_attr(p_attr, 'pi', table)
    print(idx,is_table,is_aside,float_right,depth_table,tf_idf,no_rows,max_no_col,texttag_ratio,has_th,has_img,
          has_infobox)
    #dataset.append((idx,is_table,is_aside,float_right,depth_table,tf_idf,no_rows,max_no_col,texttag_ratio,has_th,
                    #has_img,has_infobox))


#uniformity in class/attrs for table children
#what surrounds it
#position and size(area)?
#structured learning
#size of table
#alignment
#similarity in visuals , class, attrs, tags
#size of image, font of plain text. font of link text
#has text
def extractions():
    html = urllib.urlopen('https://www.kaggle.com/c/whats-cooking')
    soup = BeautifulSoup(html)
    tables = soup.findAll('table')
    #tables.append(soup.find('aside'))
    print(len(tables))
    for e,table in enumerate(tables):
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        build_table_row(e,table,soup)
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
