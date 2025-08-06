from faker import Faker

faker = Faker(locale="pt-br")

set_cnpj = {"".join(filter(str.isdigit, faker.cnpj())) for i in range(10000)}

with open("cnpj/cnpjs.csv", "w+") as arquivo:
    arquivo.write("ds_cnpj_usuf\n")
    for cnpj in set_cnpj:
        arquivo.write(cnpj + "\n")
