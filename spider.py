import requests
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool
import pandas as pd
Hearder={
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding':'gzip, deflate, sdch',
    'Accept-Language':'zh-CN,zh;q=0.8',
    'Cache-Control':'max-age=0',
    'Connection':'keep-alive',
    'Host':'www.amazon.cn',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
}
session=requests.session()
session.headers=Hearder
def write_data(data,id):#写数据到csv
    print("SAVE id of goods:" + str(id))
    save=pd.DataFrame(data)#格式化数据 pandasdatafrane
    save.to_csv('data.csv', mode='a', index=False, header=False, encoding="GBK")

def get_firstmsg(result):
    for i in range(1,20):
        print('get_page%d'%i)
        url = 'https://www.amazon.cn/s/ref=sr_pg_{}?fst=as%3Aon&rh=n%3A42689071%2Cn%3A106200071%2Cn%3A888483051%2Ck%3A%E7%AC%94%E8%AE%B0%E6%9C%AC%E7%94%B5%E8%84%91' \
              '&page={}&keywords=%E7%AC%94%E8%AE%B0%E6%9C%AC%E7%94%B5%E8%84%91&ie=UTF8'.format(i,i)
        content=session.get(url).content.decode()
        soup=bs(content,"lxml").find('div',id='resultsCol').find_all('li')
        print(len(soup))
        for item in soup:
            flag=1
            value={}
            try:value['asin']=item['data-asin']
            except:flag=0
            try:value['title']=item.find('h2').text
            except:flag=0
            if(flag):result.append(value)

def get_data(title,asin):
    url='https://www.amazon.cn/{}/product-reviews/{}/ref=cm_cr_arp_d_paging_btm?ie=UTF8&reviewerType=all_reviews&pageNumber={}'.format(title,asin,1)
    content=session.get(url).content.decode()
    result={}
    try:
        flag=1
        soup=bs(content,'lxml')
        items=soup.find('div',id="cm_cr-product_info")
        result['title'] = title
        result['score'] = items.find('span', class_='a-size-medium totalReviewCount').text
        result['price'] = items.find('div', {'class': 'a-row product-price-line'}).find('span','a-color-price arp-price').text[2:]
        result['brand'] = items.find('a', {'class': 'a-size-base a-link-normal'}).text
        comment_list=[]
        i=1
        while True:
            url = 'https://www.amazon.cn/{}/product-reviews/{}/ref=cm_cr_arp_d_paging_btm?ie=UTF8&reviewerType=all_reviews&pageNumber={}'\
                .format(title, asin, i)
            content = session.get(url).content.decode()
            comment=bs(content, 'lxml').find('div',{'id':'cm_cr-review_list','class':'a-section a-spacing-none review-views celwidget'}).find_all('div',{'class':'a-section celwidget'})
            if(len(comment)==0):break
            for item in comment:
                value={}
                value['score']=item.find('a',class_='a-link-normal')['title']
                value['author']=item.find('a',{'data-hook':'review-author'}).text
                value['comment']=item.find('span',{'data-hook':"review-body"}).text
                comment_list.append(value)
            i+=1
        result['comment']=comment_list
    except:flag=0
    if(flag):#有评论
        save = []
        save.append(result)
        write_data(save, asin)

if __name__=='__main__':
    items = []
    get_firstmsg(items)
    pool=Pool(processes=10)
    with open('data.csv','w+') as file:
        file.write('品牌,评论,价格,尺码,商品名称\n')#表头
    for item in items:
        pool.apply_async(get_data,(item['title'],item['asin'],))#非阻塞多进程
    pool.close()
    pool.join()