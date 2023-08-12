file_configurations = [
    {
        "file_name": "Atviri_JTP_parko_duomenys.csv",
        "table_name": "companies_cars_raw",
        "table_schema": [
            TableSchema(name='MARKE', data_type='STRING'),
            TableSchema(name='KOMERCINIS_PAV', data_type='STRING'),
            TableSchema(name='KATEGORIJA_KLASE', data_type='STRING'),
            TableSchema(name='NUOSAVA_MASE', data_type='FLOAT'),
            TableSchema(name='GALIA', data_type='FLOAT'),
            TableSchema(name='GALIA_ELEKTR', data_type='FLOAT'),
            TableSchema(name='DEGALAI', data_type='STRING'),
            TableSchema(name='CO2_KIEKIS', data_type='INTEGER'),
            TableSchema(name='CO2_KIEKIS__WLTP', data_type='INTEGER'),
            TableSchema(name='TERSALU_LYGIS', data_type='STRING'),
            TableSchema(name='GALIOS_MASES_SANT', data_type='FLOAT'),
            TableSchema(name='PIRM_REG_DATA', data_type='DATE'),
            TableSchema(name='PIRM_REG_DATA_LT', data_type='DATE'),
            TableSchema(name='KODAS', data_type='STRING'),
            TableSchema(name='PAVADINIMAS', data_type='STRING'),
            TableSchema(name='SAVIVALDYBE', data_type='STRING'),
            TableSchema(name='APSKRITIS', data_type='STRING')]
    },
    {
        "file_name": "monthly-2023.csv",
        "table_name": "employees_salaries_raw",
        "table_schema": [
            TableSchema(name='SOME_FIELD', data_type='STRING'),
            # ... (Different fields and their data types)
        ]
    }
]
