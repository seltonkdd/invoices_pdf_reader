import os
import pdfplumber
import psycopg2
import re
import pandas as pd

fullpath = 'invoices/'
# VARIABLES FOR YOUR DATABASE
db_name = 'postgres'
schema = ''   # ONLY FOR POSTGRESQL
db_user = 'root'
password = ''
host = 'localhost'
port = '5432'
db_table = ''

def main():
    # START
    # connection to database
    conn = psycopg2.connect(
        database=db_name,
        user=db_user,
        password=password,
        host=host,
        port=port)
    print("Connection to PostgreSQL DB successful")
    cursor = conn.cursor()
    sql = (f'INSERT INTO {schema}.{db_table}(invoice_file, cnpj, invoice_value, invoice_date, email, status)'
           ' VALUES (%s, %s, %s, %s, %s, %s)')

    files = os.listdir(fullpath)
    files_quantity = len(files)

    df = pd.DataFrame(columns=['Arquivo', 'CNPJ', 'Valor', 'Data', 'Email', 'Status'])

    # check if directory is not empty
    if files_quantity == 0:
        raise Exception('No files found in the directory.')

    # MAIN WORK
    for file in files:
        try:
            cnpjPat = r'(\d{2}\.\d{3}\.\d{3}/\d{4}\-\d{2})'
            valuePat = r'R\$[\d\.]{0,}\,\d{2}'
            datePat = r'(\d{2}/\d{2}/\d{4})'
            emailPat = r'[\w]{1,}@[\w]{1,}\.[comCOM][\.\w]{0,4}'

        # check PDF file
            with pdfplumber.open(fullpath + file) as pdf:
                page = pdf.pages[0]
                text = page.extract_text()

            matchCnpj = re.search(cnpjPat, text)
            if matchCnpj:
                cnpj = matchCnpj.group(0)
            else:
                raise Exception("Coudn't find CNPJ.")
            matchValue = re.search(valuePat, text)
            if matchValue:
                value = matchValue.group(0)
            else:
                raise Exception("Coudn't find Value number.")

            matchDate = re.search(datePat, text)
            if matchDate:
                date = matchDate.group(0)
            else:
                raise Exception("Date not found.")

            matchEmail = re.search(emailPat, text)
            if matchEmail:
                email = matchEmail.group(0)
            else:
                raise Exception("Email not found.")

            file_status = 'Completed'

            # insert values on database
            cursor.execute(sql, (file, cnpj, value, date, email, file_status))
            conn.commit()

            df = df._append({'Arquivo': file,
                             'CNPJ': cnpj,
                             'Valor': value,
                             'Data': date,
                             'Email': email,
                             'Status': file_status},
                            ignore_index=True)

        except Exception as e:
            print(f'Error processing file: {file} - {e}')
            file_status = f'Erro: {e}'
            df = df._append({'Arquivo': file, 'Status': file_status}, ignore_index=True)
            cursor.execute(sql, (file, 'N/A', 'N/A', 'N/A', 'N/A', file_status))
            conn.commit()

    df.to_excel('Notas fiscais.xlsx', index=False)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
