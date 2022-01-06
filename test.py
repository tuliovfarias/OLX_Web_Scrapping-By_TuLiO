import re, sys
text='Fonte'
titulo_anuncio='Fonte Dell- Modelo N- 305p-00 -305w- Semi Nova.'
# print(re.match(f'{text.lower()}', titulo_anuncio.lower()))
if re.match(f'{text.lower()}', titulo_anuncio.lower()):
    print('yes')