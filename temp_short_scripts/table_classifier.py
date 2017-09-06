import urllib
from bs4 import BeautifulSoup
import nltk, string
from sklearn.feature_extraction.text import TfidfVectorizer
import re
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


dataset = []


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


def extractions():
    html = urllib.urlopen('http://blog.yhat.com/posts/logistic-regression-and-python.html')
    soup = BeautifulSoup(html)
    print(soup.text)
    tables = soup.findAll('table')
    #tables.append(soup.find('aside'))
    print(len(tables))
    for e,table in enumerate(tables):
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        build_table_row(e,table,soup)
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


def build_dataset():
#heroes wiki simba
    dataset.append((0,0, 1, 0, 0, 11, 0.087771285246253522, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,1, 1, 0, 0, 16, 0.1201858813450413, 1, 1, 1.0, 0, 0, 0))
    dataset.append((0,2, 1, 0, 0, 16, 0.002750325523415453, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,3, 1, 0, 0, 16, 0.002750325523415453, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,4, 1, 0, 1, 11, 0.5420386814116398, 2, 1, 2.0, 1, 1, 1))
    dataset.append((1,5, 1, 0, 1, 15, 0.5420386814116398, 16, 2, 2.0, 1, 1, 1))
    dataset.append((0,6, 1, 0, 0, 11, 0.19925991463061882, 2, 3, 1.0, 0, 0, 0))
#https://en.wikipedia.org/wiki/Simba
    dataset.append((1,0, 1, 0, 0, 8, 0.42458622934713836, 13, 1, 1.0, 1, 1, 1))
    dataset.append((0,1, 1, 0, 0, 9, 0.57062430407753484, 8, 1, 1.0, 1, 0, 0))
    dataset.append((0,2, 1, 0, 0, 12, 0.34149496973950805, 3, 1, 1.0, 1, 0, 0))
#simba 2017 movie wikipedia
    dataset.append((1,0, 1, 0, 0, 8, 0.36652499531252591, 10, 1, 0.0, 1, 0, 1))
    dataset.append((0,1, 1, 0, 0, 8, 0.2981044346136944, 1, 2, 1.0, 0, 1, 0))
#http: // wikitravel.org / en / China
    dataset.append((0,0, 1, 0, 0, 7, 0.99964667329711676, 1, 2, 11.0, 1, 1, 0))
    dataset.append((1,1, 1, 0, 1, 11, 0.10013649559156801, 18, 2, 1.0, 0, 1, 0))
    dataset.append((0,2, 1, 0, 0, 11, 0.061834353934683034, 1, 2, 3.0, 0, 1, 0))
    dataset.append((0,3, 1, 0, 1, 10, 0.040571082636077456, 1, 1, 3.0, 0, 0, 1))
    dataset.append((0,4, 1, 0, 0, 11, 0.14088726083606309, 1, 2, 2.0, 0, 0, 0))
    dataset.append((0,5, 1, 0, 0, 11, 0.21136181153474723, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,6, 1, 0, 0, 11, 0.19989470899049444, 1, 2, 2.0, 0, 0, 0))
    dataset.append((0,7, 1, 0, 0, 11, 0.13237699076480292, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,8, 1, 0, 0, 11, 0.13107882226474474, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,9, 1, 0, 0, 11, 0.19190142712890967, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,10, 1, 0, 0, 11, 0.24883888282302374, 1, 2, 3.0, 0, 0, 0))
    dataset.append((0,11, 1, 0, 0, 11, 0.25265406935916224, 1, 1, 17.0, 0, 0, 0))
    dataset.append((0,12, 1, 0, 1, 10, 0.11585075928167769, 1, 1, 3.0, 0, 0, 1))
    dataset.append((0,13, 1, 0, 1, 10, 0.11059814176547589, 1, 1, 21.0, 0, 0, 1))
    dataset.append((0,14, 1, 0, 0, 10, 0.068070954847852805, 23, 7, 1.0, 1, 0, 0))
    dataset.append((0,15, 1, 0, 1, 10, 0.21291191337773374, 1, 1, 20.0, 0, 0, 1))
    dataset.append((0,16, 1, 0, 1, 10, 0.1540625093464322, 1, 1, 29.0, 0, 0, 1))
    dataset.append((0,17, 1, 0, 1, 10, 0.02696431430169733, 1, 1, 1.0, 0, 0, 1))
    dataset.append((0,18, 1, 0, 1, 10, 0.16569273526249595, 1, 1, 19.0, 0, 0, 1))
    dataset.append((0,19, 1, 0, 0, 11, 0.21110655814636742, 2, 2, 22.0, 0, 1, 0))
    dataset.append((0,20, 1, 0, 1, 10, 0.058886397641215213, 1, 1, 2.0, 0, 0, 1))
    dataset.append((0,21, 1, 0, 0, 11, 0.17838803388813759, 2, 2, 8.0, 0, 1, 0))
    dataset.append((0,22, 1, 0, 1, 10, 0.11699385351970316, 1, 1, 4.0, 0, 0, 1))
    dataset.append((0,23, 1, 0, 1, 10, 0.24994497068343002, 1, 1, 7.0, 0, 0, 1))
#http://lionking.wikia.com/wiki/Simba
    dataset.append((0,0, 1, 0, 0, 11, 0.023551516867261032, 1, 2, 1.0, 0, 1, 0))
    dataset.append((0,1, 1, 0, 0, 11, 0.42811902295551552, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,2, 1, 0, 0, 16, 0.60336268147396854, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,3, 1, 0, 0, 16, 0.0017934984814907747, 1, 1, 0.0, 0, 0, 0))
    dataset.append((1,4, 1, 0, 1, 11, 0.38951803171055149, 16, 2, 0.0, 1, 1, 1))
    dataset.append((0,5, 1, 0, 0, 11, 0.32732681221775994, 1, 3, 4.0, 0, 0, 0))
    dataset.append((0,6, 1, 0, 0, 11, 0.22834371409611054, 1, 3, 2.0, 0, 0, 0))
    dataset.append((0,7, 1, 0, 0, 11, 0.38254613234369994, 1, 3, 2.0, 0, 0, 0))
    dataset.append((0,8, 1, 0, 0, 11, 0.2549731485925722, 1, 3, 3.0, 0, 0, 0))
    dataset.append((0,9, 1, 0, 0, 11, 0.065443377712582573, 1, 3, 2.0, 0, 0, 0))
    dataset.append((0,10, 1, 0, 0, 11, 0.14743399621507106, 1, 3, 8.0, 0, 0, 0))
    dataset.append((0,11, 1, 0, 0, 11, 0.19055335828303224, 1, 3, 3.0, 0, 0, 0))
    dataset.append((0,12, 1, 0, 0, 11, 0.044861680083976063, 1, 3, 1.0, 0, 0, 0))
    dataset.append((0,13, 1, 0, 0, 11, 0.208596365674173, 1, 3, 3.0, 0, 0, 0))
    dataset.append((0,14, 1, 0, 0, 11, 0.040368810049224914, 1, 3, 1.0, 0, 0, 0))
    dataset.append((0,15, 1, 0, 0, 11, 0.034704803500345743, 1, 3, 2.0, 0, 0, 0))
    dataset.append((0,16, 1, 0, 0, 11, 0.032374754309353541, 1, 3, 3.0, 0, 0, 0))
    dataset.append((0,17, 1, 0, 0, 11, 0.34118643423936512, 1, 1, 1.0, 0, 1, 0))
    dataset.append((0,18, 1, 0, 0, 11, 0.0058290049350593637, 1, 3, 1.0, 0, 0, 0))
    dataset.append((0,19, 1, 0, 0, 11, 0.099309953187038655, 1, 3, 0.0, 0, 0, 0))
    dataset.append((0,20, 1, 0, 0, 11, 0.12904674590900667, 1, 1, 3.0, 0, 0, 0))
    dataset.append((0,21, 1, 0, 0, 11, 0.079596786027180871, 9, 7, 0.0, 0, 0, 0))
    dataset.append((0,22, 1, 0, 0, 11, 0.05867525354329392, 9, 7, 0.0, 0, 0, 0))
    dataset.append((0,23, 1, 0, 0, 11, 0.18213876816037927, 2, 1, 0.0, 1, 0, 0))
    dataset.append((0,24, 1, 0, 0, 11, 0.23664507762811973, 2, 1, 0.0, 1, 0, 0))
    dataset.append((0,25, 1, 0, 0, 11, 0.17489080664761975, 2, 1, 1.0, 1, 0, 0))
    dataset.append((0,26, 1, 0, 0, 11, 0.13610600478799367, 2, 1, 0.0, 1, 0, 0))
#https://en.wikipedia.org/wiki/James_Wong_Howe
    dataset.append((0,0, 1, 0, 0, 8, 0.13145232347773061, 1, 2, 2.0, 0, 1, 0))
    dataset.append((0,1, 1, 0, 0, 8, 0.11818881415682436, 1, 2, 2.0, 0, 1, 0))
    dataset.append((1,2, 1, 0, 0, 8, 0.34567904436500735, 10, 1, 1.0, 1, 1, 1))
    dataset.append((0,3, 1, 0, 0, 9, 0.26375020222321488, 6, 1, 1.0, 1, 0, 0))
    dataset.append((0,4, 1, 0, 0, 9, 0.04357758497611508, 1, 1, 0.0, 1, 0, 0))

#https://es.wikipedia.org/wiki/Markdown
    dataset.append((1,0, 1, 0, 0, 8, 0.63175091728687605, 18, 1, 1.0, 1, 1, 1))

#https://stackoverflow.com/questions/40615068/beautiful-soup-4-string-nonetype-object-is-not-callable
    dataset.append((0,0, 1, 0, 0, 10, 0.40794876279764664, 2, 2, 1.0, 0, 1, 0))
    dataset.append((0,1, 1, 0, 0, 14, 0.12570719084821902, 1, 3, 0.0, 0, 1, 0))
    dataset.append((0,2, 1, 0, 0, 14, 0.0, 0, 0, 0.0, 0, 0, 0))
    dataset.append((0,3, 1, 0, 0, 11, 0.44869356323636106, 2, 2, 1.0, 0, 1, 0))
    dataset.append((0,4, 1, 0, 0, 14, 0.12607802879566757, 1, 3, 0.0, 0, 1, 0))
    dataset.append((0,5, 1, 0, 0, 15, 0.13362951535710815, 0, 0, 1.0, 0, 0, 0))
    dataset.append((0,6, 1, 0, 0, 19, 0.0, 0, 0, 0.0, 0, 0, 0))
    dataset.append((0,7, 1, 0, 0, 20, 0.035520908327532026, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,8, 1, 0, 0, 16, 0.035520908327532026, 1, 1, 0.0, 0, 0, 0))
    dataset.append((0,9, 1, 0, 0, 10, 0.048817794066595734, 3, 2, 0.0, 0, 0, 0))

#http://pxtoem.com/
    dataset.append((0,0, 1, 0, 0, 9, 0.2923440064017499, 0, 0, 0.0, 1, 0, 0))
    dataset.append((0,1, 1, 0, 0, 9, 0.2923440064017499, 0, 0, 0.0, 1, 0, 0))

#https://github.com/dbpedia/extraction-framework/search?p=7&q=infobox&type=&utf8=%E2%9C%93
    dataset.append((0,0, 1, 0, 0, 14, 0.13531488329057767, 4, 2, 1.0, 0, 0, 0))
    dataset.append((0,1, 1, 0, 0, 14, 0.19762003470701994, 5, 2, 4.0, 0, 0, 0))
    dataset.append((0,2, 1, 0, 0, 14, 0.13153273688003694, 7, 2, 1.0, 0, 0, 0))
    dataset.append((0,3, 1, 0, 0, 14, 0.15543988033591791, 3, 2, 2.0, 0, 0, 0))
    dataset.append((0,4, 1, 0, 0, 14, 0.39918706992701447, 1, 2, 5.0, 0, 0, 0))
    dataset.append((0,5, 1, 0, 0, 14, 0.13677255580888736, 6, 2, 1.0, 0, 0, 0))
    dataset.append((0,6, 1, 0, 0, 14, 0.055529516233861355, 5, 2, 1.0, 0, 0, 0))
    dataset.append((0,7, 1, 0, 0, 14, 0.088997139036927422, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,8, 1, 0, 0, 14, 0.085180859860252034, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,9, 1, 0, 0, 14, 0.081196451220793622, 1, 2, 1.0, 0, 0, 0))
    dataset.append((0,10, 1, 0, 0, 9, 0.48633165728202027, 0, 0, 2.0, 1, 0, 0))
    dataset.append((0,11, 1, 0, 0, 10, 0.45132569866139077, 0, 0, 3.0, 1, 0, 0))
    dataset.append((0,12, 1, 0, 0, 10, 0.52381740089127771, 0, 0, 3.0, 1, 0, 0))
    dataset.append((0,13, 1, 0, 0, 10, 0.36791084774339877, 0, 0, 2.0, 1, 0, 0))
    dataset.append((0,14, 1, 0, 0, 10, 0.41539956305579551, 0, 0, 3.0, 1, 0, 0))

if __name__ == "__main__":
    extractions()
    build_dataset()
    print(dataset.__len__())
    #classify()
