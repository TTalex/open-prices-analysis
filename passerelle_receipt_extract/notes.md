## Need

Given a receipt (3k total), the project is looking for:
* Location
* Is the product organic
* price per kilo (if written)

## Looking at categories (bulk products)

### What we have
We have an organic field, but prompt suggests that it should be set to False, because data is rarely shown (hypothesis 1).

```python
    organic: bool = Field(
        False,
        description="true if the product entry is organic. Receipts rarely show "
        "enough detail to detect this from the image; this is not extracted from "
        "the image and defaults to false, but may be set afterwards based on "
        "shop context (e.g. an organic-only store).",
    )
```

It gets updated to True if attached to a store with an osm tag `organic:only`.

### Let's check hypothesis 1

Let's find category prices from receipts, filter only the ones that have the 'en:organic' flag and compare the ones that have "bio" or "org" in the name with the ones that don't.

https://huggingface.co/datasets/openfoodfacts/open-prices/viewer/default/prices?views%5B%5D=prices&sql=--+The+SQL+console+is+powered+by+DuckDB+WASM+and+runs+entirely+in+the+browser.%0A--+Get+started+by+typing+a+query+or+selecting+a+view+from+the+options+below.%0ASELECT+count%28*%29%2C+%28lower%28product_name%29+like+%27%25bio%25%27+or+lower%28product_name%29+like+%27%25org%25%27%29+as+has_bio_in_name+FROM+prices+where+type+%3D+%27CATEGORY%27+and+product_name+not+null+and+proof_type+%3D+%27RECEIPT%27+and+%27en%3Aorganic%27+in+labels_tags+group+by+has_bio_in_name+%3B

```sql
SELECT count(*), (lower(product_name) like '%bio%' or lower(product_name) like '%org%') as has_bio_in_name FROM prices where type = 'CATEGORY' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags group by has_bio_in_name ;
```
| count_star() | has_bio_in_name |
| --- | --- |
| 143 | true |
| 375 | false |

With simple filtering, we can already detect a third of the prices.

For the remaining (oragnic products that aren't labeled "bio" or "org"), let's look at the store of origin :
```sql
SELECT count(*), split_part(location_osm_display_name, ',', 1) as store_name FROM prices where type = 'CATEGORY' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags and not (lower(product_name) like '%bio%' or lower(product_name) like '%org%') group by store_name order by count(*) desc;
```

| count_star() | store_name |
| --- | --- |
| 179 | Miyam |
| 42 | Biocoop - Un Écrin Vert |
| 40 | Bio C' Bon |
| 22 | Biocoop |
| 17 | Les jardins de Malissoles |
| 14 | Naturalia |
| 13 | Casabio |
| 10 | Satoriz Saint-Martin-d'Hères |
| 5 | Bio N You |
| 4 | La Clef des Champs |
| 4 | L'Éléfàn |
| 3 | Whole Foods Market |
| 3 | Le Petit Ravito |
| 3 | färm |
| 3 | L'Eau Vive |
| 2 | BioCoop |
| 2 | K-Market |
| 2 | La Vie Claire |
| 2 | Satoriz Échirolles |
| 1 | Delhaize |
| 1 | Lidl |
| 1 | Walmart Supercenter |
| 1 | Franprix |
| 1 | ALDI |


Might be risky to generalize, but most of these are actually selling only organic products, so with a filter on known store name, we probably can catch most of the organic products.

-> Removing bio only stores, that leads to 70/513 = 13% of detections errors, or 86% of success.

## Looking a barcode products
(All queries run on duckdb, since it requires a merge with off table)

```sql
SELECT count(*), (lower(product_name) like '%bio%' or lower(product_name) like '%org%') as has_bio_in_name FROM prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 group by has_bio_in_name ;
```


| count_star() | has_bio_in_name |
| --- | --- |
|         1287 | true            |
|          913 | false           |

-> 1287/(1287+913) = 58% of organic products have a bio or org in the name.

```sql
SELECT count(*), split_part(location_osm_display_name, ',', 1) as store_name FROM prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 and not (lower(product_name) like '%bio%' or lower(product_name) like '%org%') group by store_name order by count(*) desc;
```
| count_star() |         store_name          |
| --- | --- |
|          225 | Biocoop                     |
|           63 | Walmart Supercenter         |
|           58 | Bio C' Bon                  |
|           50 | Costco                      |
|           44 | Carrefour Market            |
|           41 | Naturalia                   |
|           38 | Biocoop - Un Écrin Vert     |
|           37 | Whole Foods Market          |
|           26 | Delhaize                    |
|           21 | Carrefour City              |
|           19 | L'Éléfàn                    |
|           17 | Intermarché                 |
|           17 | Monoprix                    |
|           15 | Willys Landskrona Infarten  |
|           14 | Sainbio'z                   |
|           13 | Franprix                    |
|           13 | Satoriz Échirolles          |
|           12 | NULL                        |
|           12 | färm                        |
|           11 | REWE                        |
|            · |  ·                          |
|            · |  ·                          |
|            · |  ·                          |
|            1 | Grocery Outlet              |
|            1 | Esselunga                   |
|            1 | 8 Ave (EB) at McBride Blvd  |
|            1 | Ekoplaza                    |
|            1 | Asi Mart                    |
|            1 | BioCoop                     |
|            1 | Ets Coquet                  |
|            1 | Satoriz Albertville         |
|            1 | ALDI Nord                   |
|            1 | Vocarna.hr                  |
|            1 | Supeco                      |
|            1 | Proxy Delhaize              |
|            1 | Proxi                       |
|            1 | Centre Commercial E.Leclerc |
|            1 | Marché U                    |
|            1 | PENNY                       |
|            1 | Musée Clément Faugier       |
|            1 | Сільпо                      |
|            1 | REWE Center                 |
|            1 | Coop                        |

Sadly, quite a few of these stores don't play nice, and sell both organic and non-organic products.

-> Removing bio only stores, that leads to ~530/2200 = 24% of detections errors, or 76% of success.

Example of labelled organic products at Carrefour Market :

```sql
select split_part(location_osm_display_name, ',', 1) as store_name, product_code, product_name from prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 and not (lower(product_name) like '%bio%' or lower(product_name) like '%org%') and store_name = 'Carrefour Market' group by *;
```
|    store_name    | product_code  |    product_name     |
| --- | --- | --- |
| Carrefour Market | 3560071190965 | 500G FLOCON AVOINE  |
| Carrefour Market | 6191509909940 | 75CL HUILE DOLIVE   |
| Carrefour Market | 3560071182823 | *8X10G BOUILLON LEG |
| Carrefour Market | 3760091723574 | CHOC NR CARAM SEL   |
| Carrefour Market | 3560071449018 | 500G MELANGE FRUIT  |
| Carrefour Market | 3274936603704 | 1L JUS MULTIFRUITS  |
| Carrefour Market | 3560071433352 | 200G BISC AVOIN CH  |
| Carrefour Market | 3259011034000 | 2X125G TOFU NATURE  |
| Carrefour Market | 3560071469542 | 130G GALETTE RIZ C  |
| Carrefour Market | 3560070939244 | 110G MINICRACK EPE  |
| Carrefour Market | 3560071242589 | 230G PATE BRISEE B  |
| Carrefour Market | 3270190020998 | 20CL CREME FRAICHE  |
| Carrefour Market | 3560071438951 | CAR APP SAP 90CL    |
| Carrefour Market | 3245412304941 | 100G CHOCO NOIR PO  |
| Carrefour Market | 3560071264734 | 140G 4TRCSN JAM SU  |
| Carrefour Market | 3560071237875 | 4X100G CMPT POM/CH  |
| Carrefour Market | 3560071262266 | 1L VINAIGRE ALC BL  |
| Carrefour Market | 3456300016369 | 1L VEL PETIT POIS   |
| Carrefour Market | 3700214614259 | 100G CHOCO NOIR 75  |
| Carrefour Market | 3560071139346 | 72CL HAR.VERTS XF   |
| Carrefour Market | 3560071252526 | BRK 1L PJ ORANG AV  |
| Carrefour Market | 3560070119851 | CAR EMM RAPE 2006   |
| Carrefour Market | 3289196040664 | 1L FRT RGE HIBIS B  |
| Carrefour Market | 3560070976775 | 150G 6 FINANCIERS   |
| Carrefour Market | 3560070820603 | 1L PUR JUS MULTIFR  |
| Carrefour Market | 3760278868623 | 100G CHOCO COCO PA  |
| Carrefour Market | 3560071267070 | X 20 INFUSION RELA  |
| Carrefour Market | 3560071407568 | 500G MUELSI 35% FR  |
| Carrefour Market | 3560070379323 | 25CL SOJA A CUISIN  |
| Carrefour Market | 3259868651283 | 75IGP ATL.BL SAUV.  |
| Carrefour Market | 3560070519309 | 500GR FLOCONS 5 CE  |
| Carrefour Market | 3560071422608 | 3X20CL SOJA CUISIN  |
| Carrefour Market | 3560070370153 | 200G TABOULE ORIEN  |
| Carrefour Market | 3245390037794 | 6 OEUFS RDF         |
| Carrefour Market | 3560071442187 | *88G BOUILLON CUBE  |
| Carrefour Market | 3270190128717 | 1L BOISSON SOJA NA  |
| Carrefour Market | 3560070720859 | 80G CHOC.NOIR 72%   |
| Carrefour Market | 6191509900855 | HUILE O.VIERGE EXT  |
| Carrefour Market | 3760020500559 | LAIT DE COCO        |

None of these are obviously organic from the written name on receipt.

### What about a focus on fruits and vegetables ?

```sql
SELECT count(*), (lower(product_name) like '%bio%' or lower(product_name) like '%org%') as has_bio_in_name FROM prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 and ('en:vegetables' in categories_tags or 'en:fruits' in categories_tags) group by has_bio_in_name ;
```
| count_star() | has_bio_in_name |
|-------------:|-----------------|
| 222          | true            |
| 56           | false           |

-> 222/(222+56) = 80% of success.

```sql
select split_part(location_osm_display_name, ',', 1) as store_name, product_code, product_name from prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 and not (lower(product_name) like '%bio%' or lower(product_name) like '%org%') and ('en:vegetables' in categories_tags or 'en:fruits' in categories_tags)  group by * order by store_name;
```

|         store_name         | product_code  |            product_name             |
|----------------------------|---------------|-------------------------------------|
| Auchan Supermarché         | 3413930300046 | TOMATE RDE GRAPPE                   |
| Bio C' Bon                 | 3329489901613 | TOMATES CONCASSEES 400G (240G P     |
| Bio C' Bon                 | 3329489901613 | TOMATES CONCASSEES 400G             |
| Biocoop                    | 3760265420124 | POIME SACHET G                      |
| Carrefour City             | 3276555704107 | FILET 1 KG ORANGE BI                |
| Carrefour City             | 3523680438224 | BANANE 310                          |
| Carrefour Market           | 3560071139346 | 72CL HAR.VERTS XF                   |
| Carrefour Market           | 3560071449018 | 500G MELANGE FRUIT                  |
| Costco                     | 0096619140497 | KS BLUEBERRY                        |
| Costco                     | 0097923002365 | PITTED DATES                        |
| Costco                     | 0096619140466 | KS BLUEBERRY                        |
| Costco                     | 0097923002365 | 7099 PITTED DATES                   |
| Costco                     | 0885206005321 | OR BLUES                            |
| Costco Business Center     | 6291108636923 | DATTES                              |
| Delhaize                   | 5400117573629 | CITRONS 500GR BID                   |
| Delhaize                   | 5400119587518 | CELERI VERT                         |
| E. Leclerc                 | 3760020501297 | DOUBLE CONCENTRE TOMATE JB 140G     |
| E.Leclerc                  | 3564707185486 | HARICOTS VERTS BIJ VIL,220G         |
| E.Leclerc                  | 3564707117555 | CAROTTE BV CAT-2- SACHET 1 KG       |
| Esselunga                  | 8005110060106 | MUTTI PELATI 400X2                  |
| Intermarché                | 3250391292011 | ST ELOI BETTERAV ENT                |
| Intermarché                | 3250392253462 | ENDIVE 3/5 FRUITS BI                |
| Intermarché                | 3701147200151 | BANANE RUBAN 5 FRUIT                |
| Intermarché Super Bresson  | 3250392055257 | ST ELOI TOM ENT PELE                |
| K-Market                   | 6410405227065 | Pirkka paseer tomaatti 500g Lu      |
| L'Eau Vive                 | 3760295171454 | TOMATES CONCASS 400G                |
| L'Éléfàn                   | 3760020501297 | DOUBLE CONCENTRE DE TOMATE          |
| L'Éléfàn                   | 3329489901583 | TOMATES PELEES                      |
| Lidl                       | 20242121      | Rajčica roma 500g                   |
| REWE                       | 4337256353014 | TOMATEN STUECKIG                    |
| Walmart Supercenter        | 0024000257967 | TR FR DC TOM                        |
| Walmart Supercenter        | 0078742112138 | GVO BLUE                            |
| Walmart Supercenter        | 0681131098984 | KALE                                |
| Whole Foods Market         | 0715756100156 | OG RED RASPBRRY                     |
| Whole Foods Market         | 0715756100040 | DRSCL OG RASPBERRIES 6OZ            |
| Whole Foods Market         | 0715756300211 | DRSCL OG BLUEBERRIES                |
| Whole Foods Market         | 0099482465728 | 365WFM OG CARROT 2LB                |
| Whole Foods Market         | 0812049009209 | OG BLUEBERRIES                      |
| Whole Foods Market         | 0655852006399 | AURPRD OG SPLIT GREEN PEA           |
| Willys Landskrona Infarten | 7340083466830 | BABYSPENAT 175G                     |
| Willys Landskrona Infarten | 7340083466854 | SMÅBLADSMIX EKO                     |
| Willys Landskrona Infarten | 7340083466861 | SMÅBLADSMIX EKO                     |
| Willys Landskrona Infarten | 7311041049952 | MOROT EKO 1KG                       |
| NULL                       | 3760020509514 | Pulpe de tomate Jardin Blo-Blo-500g |


```sql
SELECT count(*), split_part(location_osm_display_name, ',', 1) as store_name FROM prices.prices where type = 'PRODUCT' and product_name not null and proof_type = 'RECEIPT' and 'en:organic' in labels_tags_1 and not (lower(product_name) like '%bio%' or lower(product_name) like '%org%') and ('en:vegetables' in categories_tags or 'en:fruits' in categories_tags) group by store_name order by count(*) desc;
```

| count_star() |         store_name         |
|-------------:|----------------------------|
| 9            | Costco                     |
| 8            | Whole Foods Market         |
| 5            | Walmart Supercenter        |
| 4            | Willys Landskrona Infarten |
| 3            | L'Éléfàn                   |
| 3            | Bio C' Bon                 |
| 3            | Delhaize                   |
| 3            | Carrefour Market           |
| 3            | Intermarché                |
| 2            | E.Leclerc                  |
| 2            | Carrefour City             |
| 1            | Lidl                       |
| 1            | K-Market                   |
| 1            | REWE                       |
| 1            | E. Leclerc                 |
| 1            | NULL                       |
| 1            | Intermarché Super Bresson  |
| 1            | Biocoop                    |
| 1            | Esselunga                  |
| 1            | L'Eau Vive                 |
| 1            | Auchan Supermarché         |
| 1            | Costco Business Center     |

-> Removing bio only stores, that leads to 52/278 = 19% of detections errors, or 81% of success.

## Summary

| Type of product | Success rate after filter on name | Success rate after filter on store |
|------------|------------------------|------------------------------|
| Category/bulk | 33% | 86% |
| Barcode product | 58% | 76% |
| Barcode fruits and vegetables | 80% | 81% |

According to this data, hypothesis 1 is mostly true : using only the name written on receipts, only a third of organic bulk product are written as organic, and a bit more than half of the barocde products are written as organic.

However, including a second parameter as the store characteristics, success rate becomes decent.

Sample size isn't huge, and very much biased on price availability. But this leads me to belive that we can get a pretty good results to Passerelle, with the following conditions :
* Prompt doesn't have to change
* Simple regex rules on written product name should be used ("%bio%" or "%org%")
* Store names have to be correctly detected (they are not inputted when receipt are uploaded)
* A list of fully organic (or mostly organic) french stores should be provided



