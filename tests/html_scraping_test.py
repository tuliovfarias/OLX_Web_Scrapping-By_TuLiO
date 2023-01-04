from html_scraping import BuscaProduto

url='https://mg.olx.com.br/belo-horizonte-e-regiao/animais-de-estimacao/cachorros/filhotes-de-maltes-e-shih-tzu-macho-e-femea-1056693480?lis=listing_no_category'

a = BuscaProduto.OLX_pesquisa_prod(url_produto=url)

print(a)