from pyparsing import *

text = """
Output folder: /dlabdata1/braemy/wikipedia_classification/data2017-05-24_09-53-59-252590
{{see also|Portal:Algeria{{!}}Algeria portal|Outline of Algeria}}
{{pp-move|small=yes}}
{{pp-move-indef}}
{{Use dmy dates|date=December 2016}}
{{coord|28|N|2|E|scale:10000000_type:country_region:DZ|format=dms|display=title}}
{{Infobox country
|conventional_long_name = People's Democratic Republic of Algeria
|native_name     = ''{{small|{{native name|ar|{{noitalic|الجمهورية الجزائرية الديمقراطية الشعبية}}}}}}<br>{{small|{{native name|ber|ⵟⴰⴳⴷⵓⴷⴰ ⵜⴰⵎⴻⴳⴷⴰⵢⵜ ⵜⴰⵖⴻⵔⴼⴰⵏⵜ ⵜⴰⵣⵣⴰⵢⵔⵉⵜ}}}}''<br/>''{{small|{{native name|fr|{{noitalic|République démocratique populaire d'Algérie}}}}}}
|common_name     = Algeria
|image_flag      = Flag of Algeria.svg
|image_coat      = Algeria emb (1976).svg
|symbol_type     = Emblem
|national_motto  = ''{{small|بالشّعب وللشّعب}}''<br>By the people and for the people<ref name="CONST-AR">{{cite web|url=http://www.el-mouradia.dz/arabe/symbole/textes/constitution96.htm |title=Constitution of Algeria, Art. 11 |language=Arabic |publisher=El-mouradia.dz |accessdate=17 January 2013 |deadurl=yes |archiveurl=https://web.archive.org/web/20120718124116/http://www.el-mouradia.dz/arabe/symbole/textes/constitution96.htm |archivedate=18 July 2012 |df=dmy }}</ref><ref name="CONST-EN">{{cite web|url=http://www.apn-dz.org/apn/english/constitution96/titre_01.htm |title=Constitution of Algeria; Art. 11 |publisher=Apn-dz.org |date=28 November 1996 |accessdate=17 January 2013 |deadurl=yes |archiveurl=https://web.archive.org/web/20130725130249/http://www.apn-dz.org/apn/english/constitution96/titre_01.htm |archivedate=25 July 2013 }}</ref>
|national_anthem = ''[[Kassaman]]''<br />({{lang-en|"We Pledge"}})<br /><center>[[File:National anthem of Algeria, by the U.S. Navy Band.oga]]</center>
|image_map       = Algeria_(orthographic_projection).svg
|map_caption     = {{map caption |location_color=dark green || | |}}
|capital         = [[Algiers]]
|coordinates     = {{Coord|36|42|N|3|13|E|type:city}}
|largest_city    = capital
|official_languages = {{hlist |[[Arabic]]<ref name="constitution">{{cite web|url=http://www.apn-dz.org/apn/english/constitution96/titre_01.htm |title=Constitution of Algeria; Art. 3 |publisher=Apn-dz.org |date=28 November 1996 |accessdate=17 January 2013 |deadurl=yes |archiveurl=https://web.archive.org/web/20130725130249/http://www.apn-dz.org/apn/english/constitution96/titre_01.htm |archivedate=25 July 2013 }}</ref> |[[Berber languages|Berber]]<ref name="APS">{{cite web |url=http://www.aps.dz/images/doc/PROJET-DE%20REVISION-DE-LA-CONSTITUTION-28-DECEMBRE-2015.pdf|title=APS |publisher=[[Algeria Press Service]] |date=6 January 2016 |accessdate=6 January 2016}}</ref>}}
|languages_type  = Other languages
|languages       = [[French language|French]] {{small|(Business and education)}}<ref name="AlgeriaFactbook">{{cite web|url=https://www.cia.gov/library/publications/the-world-factbook/geos/ag.html |title=The World Factbook – Algeria |publisher=[[Central Intelligence Agency]] |date=4 December 2013 |accessdate=24 December 2013 |deadurl=yes |archiveurl=http://www.webcitation.org/6BNNjndve?url=https%3A%2F%2Fwww.cia.gov%2Flibrary%2Fpublications%2Fthe-world-factbook%2Fgeos%2Fag.html |archivedate=13 October 2012 |df=dmy }}</ref>
| ethnic_groups =
 {{vunblist
  | 99% {{nowrap|Arab-Berber<ref name="AlgeriaFactbook"/>{{ref label|Amazigh|a|}}<!--end nowrap:-->}}
  | 1% Others<ref name="AlgeriaFactbook"/>}}
|religion        = [[Sunni Islam]]
|demonym         = Algerian
|government_type = {{nowrap|[[Unitary state|Unitary]] [[Semi-presidential system|semi-presidential]]}} [[people's republic]]
|leader_title1   = [[President of Algeria|President]]
|leader_name1    = [[Abdelaziz Bouteflika]]
|leader_title2   = [[Prime Minister of Algeria|Prime Minister]]
|leader_name2    = [[Abdelmalek Sellal]]
|legislature     = [[Parliament of Algeria|Parliament]]
|upper_house     = [[Council of the Nation]]
|lower_house     = [[People's National Assembly]]
|sovereignty_type   = [[Algerian War|Independence]] {{nobold|from [[France]]}}
|established_event1 = Declared
|established_date1  = 3 July 1962
|established_event2 = Recognised
|established_date2  = 5 July 1962
|area_km2        = 2381741
|area_sq_mi      = 919595
|area_rank       = 10th
|percent_water   = negligible
|population_estimate = 40,400,000<ref name="ONS">{{cite web |url=http://www.ons.dz/-Demographie-.html |title=Démographie (ONS) |publisher=ONS |date=19 January 2016 |accessdate=19 January 2014}}</ref>
|population_estimate_year = 2016
|population_estimate_rank = 33rd
|population_census = 37,900,000<ref name="ONS"/>
|population_census_year = 2013
|population_density_km2 = 15.9
|population_density_sq_mi = 37.9
|population_density_rank = 208th
|GDP_PPP_year    = 2016
|GDP_PPP         = $609.394 billion<ref name=imf2>[http://www.imf.org/external/pubs/ft/weo/2016/02/weodata/weorept.aspx?pr.x=52&pr.y=14&sy=2016&ey=2021&scsm=1&ssd=1&sort=country&ds=.&br=1&c=612&s=NGDPD%2CNGDPDPC%2CPPPGDP%2CPPPPC%2CLP&grp=0&a= Algeria]. [[International Monetary Fund]]
</ref>
|GDP_PPP_rank    =
|GDP_PPP_per_capita = $14,949<ref name=imf2/>
|GDP_nominal_year = 2016
|GDP_nominal     = $168.318 billion<ref name=imf2/>
|GDP_nominal_per_capita = $4,129<ref name=imf2/>
|Gini_year       = 1995
|Gini            = 35.3
|Gini_ref        = <ref>{{cite web|author=Staff |url=https://www.cia.gov/library/publications/the-world-factbook/fields/2172.html |title=Distribution of Family Income&nbsp;– Gini Index |work=[[The World Factbook]] |publisher=[[Central Intelligence Agency]] |accessdate=1 September 2009 |archiveurl=http://www.webcitation.org/5rRcwIiYs?url=https%3A%2F%2Fwww.cia.gov%2Flibrary%2Fpublications%2Fthe-world-factbook%2Ffields%2F2172.html |archivedate=23 July 2010 |deadurl=no |df=dmy }}</ref>
|HDI_year        = 2015<!-- Please use the year to which the data refers, not the publication year -->
|HDI_change      = increase<!-- increase/decrease/steady -->
|HDI             = 0.736<!-- number only -->
|HDI_ref         = <ref name="HDI">{{cite web |url=http://hdr.undp.org/sites/default/files/hdr_2015_statistical_annex.pdf |title=2015 Human Development Report |date=14 December 2015 |accessdate=14 December 2015 |publisher=United Nations Development Programme |pages=21–25}}</ref>
|HDI_rank        = 83rd
|currency        = [[Algerian dinar|Dinar]]
|currency_code   = DZD
|time_zone       = [[Central European Time|CET]]
|utc_offset      = +1
|drives_on       = right<ref>{{cite news |last=Geoghegan |first=Tom |url=http://news.bbc.co.uk/2/hi/8239048.stm |title=Could the UK drive on the right? |publisher=BBC News |date=7 September 2009 |accessdate=14 January 2013}}</ref>
|date_format = dd/mm/yyyy
|calling_code    = [[Telephone numbers in Algeria|+213]]
|cctld           = [[.dz]]<br>''الجزائر.''
|footnote_a      = {{note|arabberberbox}} The ''[[The World Factbook|CIA World Factbook]]'' states that about 15% of Algerians, a minority, identify as Berber even though many Algerians have Berber origins. The Factbook explains that of the approximately 15% who identify as Berber, most live in the [[Kabylie]] region, more closely identify with Berber heritage instead of Arab heritage, and are Muslim.
}}
{{Contains Arabic text}}

'''Algeria''' ({{lang-ar|الجزائر}} ''{{transl|ar|al-Jazā'ir}}''; {{lang-ber|ⴷⵣⴰⵢⴻⵔ}}, ''Dzayer''; {{lang-fr|Algérie}}), officially the '''People's Democratic Republic of Algeria''', is a [[sovereign state]] in [[North Africa]] on the [[Mediterranean Sea|Mediterranean coast]]. Its capital and most populous city is [[Algiers]], located in the country's far north. With an area of {{convert|2381741|km2|sqmi|0}}, Algeria is the [[list of countries and dependencies by area|tenth-largest country in the world]], and the largest in Africa.<ref>{{cite web|url=https://www.cia.gov/library/publications/the-world-factbook/rankorder/2147rank.html|title=Country Comparison: Area |publisher=CIA World Factbook |accessdate=17 January 2013}}</ref> Algeria is bordered to the northeast by [[Tunisia]], to the east by [[Libya]], to the west by [[Morocco]], to the southwest by the [[Western Sahara]]n territory, [[Mauritania]], and [[Mali]], to the southeast by [[Niger]], and to the north by the [[Mediterranean Sea]]. The country is a [[Semi-presidential system|semi-presidential republic]] consisting of 48 provinces and 1,541 communes (counties). [[Abdelaziz Bouteflika]] has been [[President of Algeria|President]] since 1999.

"""

text = """

'''Algeria''' ( ''{{transl|ar|al-Jazā'ir}}''; , ''Dzayer''; ), officially the '''People's Democratic Republic of Algeria''', is a [[sovereign state]] in [[North Africa]] on the [[Mediterranean Sea|Mediterranean coast]]. Its capital and most populous city is [[Algiers]], located in the country's far north. With an area of , Algeria is the [[list of countries and dependencies by area|tenth-largest country in the world]], and the largest in Africa.<ref></ref> Algeria is bordered to the northeast by [[Tunisia]], to the east by [[Libya]], to the west by [[Morocco]], to the southwest by the [[Western Sahara]]n territory, [[Mauritania]], and [[Mali]], to the southeast by [[Niger]], and to the north by the [[Mediterranean Sea]]. The country is a [[Semi-presidential system|semi-presidential republic]] consisting of 48 provinces and 1,541 communes (counties). [[Abdelaziz Bouteflika]] has been [[President of Algeria|President]] since 1999.



"""

braces = nestedExpr("{{transl", "}}").suppress()
text = braces.transformString(text)
text = braces.transformString(text)

print(text)