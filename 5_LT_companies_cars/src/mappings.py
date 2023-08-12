file_configurations = [
    {
        "file_name": "Atviri_JTP_parko_duomenys.csv",
        "table_name": "companies_cars_raw",
        "table_schema": [
            {name='MARKE', data_type='STRING'},
            {name='KOMERCINIS_PAV', data_type='STRING'},
            {name='KATEGORIJA_KLASE', data_type='STRING'},
            {name='NUOSAVA_MASE', data_type='FLOAT'},
            {name='GALIA', data_type='FLOAT'},
            {name='GALIA_ELEKTR', data_type='FLOAT'},
            {name='DEGALAI', data_type='STRING'},
            {name='CO2_KIEKIS', data_type='INTEGER'},
            {name='CO2_KIEKIS__WLTP', data_type='INTEGER'},
            {name='TERSALU_LYGIS', data_type='STRING'},
            {name='GALIOS_MASES_SANT', data_type='FLOAT'},
            {name='PIRM_REG_DATA', data_type='DATE'},
            {name='PIRM_REG_DATA_LT', data_type='DATE'},
            {name='KODAS', data_type='STRING'},
            {name='PAVADINIMAS', data_type='STRING'},
            {name='SAVIVALDYBE', data_type='STRING'},
            {name='APSKRITIS', data_type='STRING'}]
    },
    {
        "file_name": "monthly-2023.csv",
        "table_name": "employees_salaries_raw",
        "table_schema": [
            {name='Juridinių asmenų registro kodas (jarCode)', data_type='STRING'},
            {name='Pavadinimas (name)', data_type='STRING'},
            {name='Savivaldybė, kurioje registruota(municipality)', data_type='STRING'},
            {name='Ekonominės veiklos rūšies kodas(ecoActCode)', data_type='STRING'},
            {name='Ekonominės veiklos rūšies pavadinimas(ecoActName)', data_type='STRING'},
            {name='Mėnuo(month)', data_type='STRING'},
            {name='Vidutinis darbo užmokestis (avgWage)', data_type='FLOAT'},
            {name='Apdraustųjų skaičius (numInsured)', data_type='FLOAT'},
            {name='periodas', data_type='DATE'}
        ]
    }
]