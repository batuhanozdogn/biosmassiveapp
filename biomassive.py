import psycopg2
from configparser import ConfigParser
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# SSL uyarılarını bastırır
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        db = dict(parser.items(section))
    else:
        raise Exception('Section {0} dosyada bulunamadı: {1}'.format(section, filename))

    return db

def connect():
    connection = None
    response = None

    try:
        params = config()
        print("PostgreSQL veritabanına bağlanılıyor...")
        connection = psycopg2.connect(**params)

        print("Veritabanına başarıyla bağlandı.")
        getir(connection)

        # make_api_request fonksiyonundan dönen response'ı al
        response = make_api_request(connection)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Veritabanı bağlantısı sonlandırıldı.')

    return response  # make_api_request fonksiyonundan gelen response'ı döndür

def getir(connection):
    try:
        imlec = connection.cursor()
        imlec.execute("SELECT * FROM report_output")
        liste = imlec.fetchall()
        print(liste)
    except (Exception, psycopg2.DatabaseError) as error:
        print('Sorgu çalıştırılırken hata oluştu:', error)
    finally:
        if imlec is not None:
            imlec.close()

def insert_data(connection, data):
    try:
        imlec = connection.cursor()

        for row in data:
            imlec.execute("""
                INSERT INTO report_output (
                    main_uploaded_variation,
                    main_existing_variation,
                    main_symbol,
                    main_af_vcf,
                    main_dp,
                    details2_dann_score,
                    links_mondo,
                    links_pheno_pubmed,
                    details2_provean
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['main_uploaded_variation'],
                row['main_existing_variation'],
                row['main_symbol'],
                row['main_af_vcf'],
                row['main_dp'],
                row['details2_dann_score'],
                row['links_mondo'],
                row['links_pheno_pubmed'],
                row['details2_provean']
            ))

        connection.commit()
        print('Veri başarıyla eklendi.')

    except (Exception, psycopg2.DatabaseError) as error:
        print('Veri eklenirken hata oluştu:', error)
    finally:
        if imlec is not None:
            imlec.close()

def make_api_request_internal(url, headers, params, connection):
    try:
        response = requests.post(url, json=params, headers=headers)
        response.raise_for_status()

        api_data = response.json()

        page = api_data.get("page", 1)
        page_size = api_data.get("page_size", 10)
        count = api_data.get("count", 0)
        results = api_data.get("results", [])

        print(f"Sayfa: {page}, Sayfa Boyutu: {page_size}, Toplam Sayı: {count}")

        for result in results:
            print(result)  # veya istediğiniz işlemleri gerçekleştirin

        insert_data(connection, results)

    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Hatası: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Bağlantı Hatası: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Zaman Aşımı Hatası: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Diğer Hata: {err}")

def make_api_request(connection):
    url = 'https://api-dev.massbio.info/assignment/query'
    headers = {'Content-Type': 'application/json'}
    params = {'filters': {'kolon_adi': 'deger', 'kolon_adi': 'deger', 'kolon_adi': 'deger'},
              'ordering': [{'kolon_adi': 'ASC'}, {'kolon_adi': 'DESC'}, {'kolon_adi': 'ASC'}]}

    # 'verify' parametresine doğru bir sertifika yolu verirseniz:
    response = requests.get(url, params=params, headers=headers, verify=False)

    if response.status_code == 200:
        print('API isteği başarıyla tamamlandı.')
        api_data = response.json()

        page = api_data.get("page", 1)
        page_size = api_data.get("page_size", 10)
        count = api_data.get("count", 0)
        results = api_data.get("results", [])

        print(f"Sayfa: {page}, Sayfa Boyutu: {page_size}, Toplam Sayı: {count}")

        for result in results:
            print(result)  # veya istediğiniz işlemleri gerçekleştirin

        insert_data(connection, results)

    elif response.status_code == 400:
        print('API isteği başarısız. Geçersiz istek gönderildi. HTTP Durum Kodu:', response.status_code)
        print(response.text)
    elif response.status_code == 500:
        print('API isteği başarısız. Sunucu tarafında bir hata oluştu. HTTP Durum Kodu:', response.status_code)
        print(response.text)
    else:
        print('Bilinmeyen bir HTTP durum kodu alındı:', response.status_code)

    make_api_request_internal(url, headers, params, connection)

if __name__ == "__main__":
    connect()
