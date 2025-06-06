import requests

url = "https://geoserver.leicester.gov.uk/geoserver/ODP/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=ODP%3AODP_ARTICLE_4_DIRECTION_AREA&maxFeatures=5000&outputFormat=application%2Fjson"

x = requests.get ('http://geoserver.leicester.gov.uk/geoserver/ODP/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=ODP%3AODP_ARTICLE_4_DIRECTION_AREA&maxFeatures=5000&outputFormat=application%2Fjson')

print (x.status_code)



session = requests.Session()
user_agent = "DLUHC Digital Land"
response = session.get(
    url,
    headers={"User-Agent": user_agent},
    timeout=120,
    verify=True,
)
print (user_agent,"        ",url)
