# İş Problemi
# Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp
# bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve
# bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

#Veri Seti Hikayesi
#Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında
# OmniChannel (hem online hem offline alışveriş yapan) olarak yapan
# müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.


#DEGİSKENLER
#master_id =Eşsiz müşteri numarası
#order_channel=Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
#last_order_channel=En son alışverişin yapıldığı kanal
#first_order_date=Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date=Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online=Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline=Müşterinin offline platformda yaptığı son alışveriş tarihi
#order_num_total_ever_online=Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline=Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline=Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online=Müşterinin online alışverişlerinde ödediği toplam ücret
#interested_in_categories_12= Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi


###############GOREV1######################
############################################################################
#ADIM1: flo_data_20K.csvverisiniokuyunuz.Dataframe’in kopyasını oluşturunuz.
############################################################################

import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.4f' % x)


df_=pd.read_csv("/Users/fadimeacikgoz/PycharmProjects/Crm Analytics/crm_analtics/datasets/flo_data_20k.csv")
df = df_.copy()
df.head()
df.shape


############################################################################
#Adım2: Verisetinde
#a. İlk 10 gözlem,
#b. Değişken isimleri,
#c. Betimsel istatistik,
#d. Boş değer,
#e. Değişken tipleri, incelemesi yapınız.
############################################################################

#a. ilk 10 gozlem
df.head(10)
#                              master_id order_channel last_order_channel first_order_date last_order_date last_order_date_online last_order_date_offline  order_num_total_ever_online  order_num_total_ever_offline  customer_value_total_ever_offline  customer_value_total_ever_online       interested_in_categories_12
# 0  cc294636-19f0-11eb-8d74-000d3a38a36f   Android App            Offline       2020-10-30      2021-02-26             2021-02-21              2021-02-26                       4.0000                        1.0000                           139.9900                          799.3800                           [KADIN]
# 1  f431bd5a-ab7b-11e9-a2fc-000d3a38a36f   Android App             Mobile       2017-02-08      2021-02-16             2021-02-16              2020-01-10                      19.0000                        2.0000                           159.9700                         1853.5800  [ERKEK, COCUK, KADIN, AKTIFSPOR]
# 2  69b69676-1a40-11ea-941b-000d3a38a36f   Android App        Android App       2019-11-27      2020-11-27             2020-11-27              2019-12-01                       3.0000                        2.0000                           189.9700                          395.3500                    [ERKEK, KADIN]
# 3  1854e56c-491f-11eb-806e-000d3a38a36f   Android App        Android App       2021-01-06      2021-01-17             2021-01-17              2021-01-06                       1.0000                        1.0000                            39.9900                           81.9800               [AKTIFCOCUK, COCUK]
# 4  d6ea1074-f1f5-11e9-9346-000d3a38a36f       Desktop            Desktop       2019-08-03      2021-03-07             2021-03-07              2019-08-03                       1.0000                        1.0000                            49.9900                          159.9900                       [AKTIFSPOR]
# 5  e585280e-aae1-11e9-a2fc-000d3a38a36f       Desktop            Offline       2018-11-18      2021-03-13             2018-11-18              2021-03-13                       1.0000                        2.0000                           150.8700                           49.9900                           [KADIN]
# 6  c445e4ee-6242-11ea-9d1a-000d3a38a36f   Android App        Android App       2020-03-04      2020-10-18             2020-10-18              2020-03-04                       3.0000                        1.0000                            59.9900                          315.9400                       [AKTIFSPOR]
# 7  3f1b4dc8-8a7d-11ea-8ec0-000d3a38a36f        Mobile            Offline       2020-05-15      2020-08-12             2020-05-15              2020-08-12                       1.0000                        1.0000                            49.9900                          113.6400                           [COCUK]
# 8  cfbda69e-5b4f-11ea-aca7-000d3a38a36f   Android App        Android App       2020-01-23      2021-03-07             2021-03-07              2020-01-25                       3.0000                        2.0000                           120.4800                          934.2100             [ERKEK, COCUK, KADIN]
# 9  1143f032-440d-11ea-8b43-000d3a38a36f        Mobile             Mobile       2019-07-30      2020-10-04             2020-10-04              2019-07-30                       1.0000                        1.0000                            69.9800                           95.9800                [KADIN, AKTIFSPOR]

#b. Değişken isimleri,
df.columns
# index(['master_id', 'order_channel', 'last_order_channel', 'first_order_date', 'last_order_date', 'last_order_date_online', 'last_order_date_offline', 'order_num_total_ever_online', 'order_num_total_ever_offline', 'customer_value_total_ever_offline', 'customer_value_total_ever_online', 'interested_in_categories_12'], dtype='object')


#c. Betimsel istatistik,
df.describe().T
#                                        count     mean      std     min      25%      50%      75%        max
# order_num_total_ever_online       19945.0000   3.1109   4.2256  1.0000   1.0000   2.0000   4.0000   200.0000
# order_num_total_ever_offline      19945.0000   1.9139   2.0629  1.0000   1.0000   1.0000   2.0000   109.0000
# customer_value_total_ever_offline 19945.0000 253.9226 301.5329 10.0000  99.9900 179.9800 319.9700 18119.1400
# customer_value_total_ever_online  19945.0000 497.3217 832.6019 12.9900 149.9800 286.4600 578.4400 45220.1300



#d. Boş değer,
df.isnull().sum()
# master_id                            0
# order_channel                        0
# last_order_channel                   0
# first_order_date                     0
# last_order_date                      0
# last_order_date_online               0
# last_order_date_offline              0
# order_num_total_ever_online          0
# order_num_total_ever_offline         0
# customer_value_total_ever_offline    0
# customer_value_total_ever_online     0
# interested_in_categories_12          0



#e. Değişken tipleri, incelemesi yapınız.
df.dtypes
# aster_id                             object
# order_channel                         object
# last_order_channel                    object
# first_order_date                      object
# last_order_date                       object
# last_order_date_online                object
# last_order_date_offline               object
# order_num_total_ever_online          float64
# order_num_total_ever_offline         float64
# customer_value_total_ever_offline    float64
# customer_value_total_ever_online     float64
# interested_in_categories_12           object
# dtype: object

############################################################################
#Adım 3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan
#alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam alışveriş
#sayısı ve harcaması için yeni değişkenler oluşturunuz.
#order_num_total_ever_online=Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline=Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline=Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online=Müşterinin online alışverişlerinde ödediği toplam ücret

df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
# 0        5.0000
# 1       21.0000
# 2        5.0000
# 3        2.0000
# 4        2.0000
#           ...
# 19940    3.0000
# 19941    2.0000
# 19942    3.0000
# 19943    6.0000
# 19944    2.0000
# Name: order_num_total, Length: 19945, dtype: float64


df["customer_value_total_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
# 0        939.3700
# 1       2013.5500
# 2        585.3200
# 3        121.9700
# 4        209.9800
#            ...
# 19940    401.9600
# 19941    390.4700
# 19942    632.9400
# 19943   1009.7700
# 19944    261.9700
# Name: customer_value_total_price, Length: 19945, dtype: float64
df.head()


############################################################################
#Adım 4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
############################################################################

#first_order_date=Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date=Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online=Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline=Müşterinin offline platformda yaptığı son alışveriş tarihi

 # 1. yol
df["first_order_date"] = df["first_order_date"].astype("datetime64")
df["last_order_date"] = df["last_order_date"].astype("datetime64")
df["last_order_date_online"] = df["last_order_date_online"].astype("datetime64")
df["last_order_date_offline"]= df["last_order_date_offline"].astype("datetime64")

# 2. yol
for i in df.columns:
    if "date" in i:
        df[i]=df[i].apply(pd.to_datetime)





############################################################################
#Adım 5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının
# ve toplam harcamaların dağılımına bakınız.
############################################################################
df.groupby("order_channel").agg({"master_id": ["count"],
                                 "order_num_total" : ["sum"],
                                  "customer_value_total_price": ["sum"]}).head()

#               master_id order_num_total customer_value_total_price
#                   count             sum                        sum
# order_channel
# Android App        9495      52269.0000               7819062.7600
# Desktop            2735      10920.0000               1610321.4600
# Ios App            2833      15351.0000               2525999.9300
# Mobile             4882      21679.0000               3028183.1600



###############################################################
#Adım 6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
###############################################################
# 1.yol
df.sort_values("customer_value_total_price", ascending=False)[:10]
# 2. yol
df.groupby(["master_id"]).agg({"customer_value_total_price": "sum"}).sort_values("customer_value_total_price", ascending=False).head(10)
#                                       customer_value_total_price
# master_id
# 5d1c466a-9cfd-11e9-9897-000d3a38a36f                  45905.1000
# d5ef8058-a5c6-11e9-a2fc-000d3a38a36f                  36818.2900
# 73fd19aa-9e37-11e9-9897-000d3a38a36f                  33918.1000
# 7137a5c0-7aad-11ea-8f20-000d3a38a36f                  31227.4100
# 47a642fe-975b-11eb-8c2a-000d3a38a36f                  20706.3400
# a4d534a2-5b1b-11eb-8dbd-000d3a38a36f                  18443.5700
# d696c654-2633-11ea-8e1c-000d3a38a36f                  16918.5700
# fef57ffa-aae6-11e9-a2fc-000d3a38a36f                  12726.1000
# cba59206-9dd1-11e9-9897-000d3a38a36f                  12282.2400
# fc0ce7a4-9d87-11e9-9897-000d3a38a36f                  12103.1500



##########################################################
#Adım 7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
##########################################################
df.groupby(["master_id"]).agg({"order_num_total": "sum"}).sort_values("order_num_total", ascending=False).head(10)
#                                       order_num_total
# master_id
# 5d1c466a-9cfd-11e9-9897-000d3a38a36f         202.0000
# cba59206-9dd1-11e9-9897-000d3a38a36f         131.0000
# a57f4302-b1a8-11e9-89fa-000d3a38a36f         111.0000
# fdbe8304-a7ab-11e9-a2fc-000d3a38a36f          88.0000
# 329968c6-a0e2-11e9-a2fc-000d3a38a36f          83.0000
# 73fd19aa-9e37-11e9-9897-000d3a38a36f          82.0000
# 44d032ee-a0d4-11e9-a2fc-000d3a38a36f          77.0000
# b27e241a-a901-11e9-a2fc-000d3a38a36f          75.0000
# d696c654-2633-11ea-8e1c-000d3a38a36f          70.0000
# a4d534a2-5b1b-11eb-8dbd-000d3a38a36f          70.0000



##########################################################
#Adım 8: Veri ön hazırlık sürecini fonksiyonlaştırınız.
#########################################################

def create_func(df):
    df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
    for i in df.columns:
        if "date" in i:
            df[i] = df[i].apply(pd.to_datetime)
    return df

df = df.copy()
dff_new= create_func(df)



######################### GOREV2 :RFM Metriklerinin Hesaplanması #########################################

#first_order_date=Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date=Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online=Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline=Müşterinin offline platformda yaptığı son alışveriş tarihi
df.describe().T
df.head()


# Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi
df["last_order_date"].max() # 2021-05-30
today_date = dt.datetime(2021,6,1)


# Recency   = "last_order_date_offline": lambda last_order_date_offline: (today_date - last_order_date_offline.max()).days
# Frequency = "interested_in_categories_12": lambda interested_in_categories_12: interested_in_categories_12.nunique()
# Monetory  = "totalPrice": lambda totalPrice: totalPrice.sum()


# customer_id, recency, frequnecy ve monetary değerlerinin yer aldığı yeni bir rfm dataframe
rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'order_num_total': lambda order_num_total: order_num_total,
                                     'customer_value_total_price': lambda customer_value_total_price: customer_value_total_price.sum()})
rfm.head()

rfm.columns = ["Recency", "Frequency", "Monetary"]
rfm.describe().T


#                count     mean      std     min      25%      50%      75%        max
# Recency   19945.0000 134.4584 103.2811  2.0000  43.0000 111.0000 202.0000   367.0000
# Frequency 19945.0000   5.0248   4.7427  2.0000   3.0000   4.0000   6.0000   202.0000
# Monetary  19945.0000 751.2443 895.4022 44.9800 339.9800 545.2700 897.7800 45905.1000



######################### GOREV3 :RF Skorunun Hesaplanmas #########################################

#Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
#Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.
#Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz

rfm["recency_score"] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["monetary_score"] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5])
#rank(method="first") // İlk gördüğünü ilk sınıfa ata
rfm["frequency_score"]= pd.qcut(rfm['Frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["RFM_SCORE"]= (rfm["recency_score"].astype(str)+
                  rfm["frequency_score"].astype(str))

rfm.describe().T

######################### Görev 4: RF Skorunun Segment Olarak Tanımlanması #########################################

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm["RFM_SCORE"].replace(seg_map, regex=True)

# master_id
# 00016786-2f5a-11ea-bb80-000d3a38a36f              champions
# 00034aaa-a838-11e9-a2fc-000d3a38a36f            hibernating
# 000be838-85df-11ea-a90b-000d3a38a36f                at_Risk
# 000c1fe2-a8b7-11ea-8479-000d3a38a36f              champions
# 000f5e3e-9dde-11ea-80cd-000d3a38a36f              champions
#                                                ...
# fff1db94-afd9-11ea-b736-000d3a38a36f                at_Risk
# fff4736a-60a4-11ea-8dd8-000d3a38a36f    potential_loyalists
# fffacd34-ae14-11e9-a2fc-000d3a38a36f                at_Risk
# fffacecc-ddc3-11e9-a848-000d3a38a36f        loyal_customers
# fffe4b30-18e0-11ea-9213-000d3a38a36f                at_Risk
# Name: segment, Length: 19945, dtype: object



###############################Görev 5: Aksiyon Zamanı !####################################
#Adım 1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm[["segment", "Recency", "Frequency", "Monetary"]].groupby("segment").agg(["mean", "count"])
#                      Recency       Frequency        Monetary
#                         mean count      mean count      mean count
# segment
# about_to_sleep      114.0316  1643    2.4066  1643  361.6494  1643
# at_Risk             242.3290  3152    4.4702  3152  648.3250  3152
# cant_loose          235.1591  1194   10.7169  1194 1481.6524  1194
# champions            17.1422  1920    8.9651  1920 1410.7089  1920
# hibernating         247.4263  3589    2.3915  3589  362.5833  3589
# loyal_customers      82.5579  3375    8.3564  3375 1216.2572  3375
# need_attention      113.0372   806    3.7395   806  553.4366   806
# new_customers        17.9762   673    2.0000   673  344.0495   673
# potential_loyalists  36.8697  2925    3.3108  2925  533.7413  2925
# promising            58.6946   668    2.0000   668  334.1533   668

rfm[rfm["RFM_SCORE"] == "45"]



# Adım 2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde.
# Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçmek isteniliyor.
# Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kurulacak müşteriler.
# Bu müşterilerin id numaralarını csv dosyasına kaydediniz.



target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
cust_ids.shape

cust_ids.head()
#b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır.
# Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler
# özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.



target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)