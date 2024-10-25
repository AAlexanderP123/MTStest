import xml.etree.ElementTree as ET
import psycopg2


'''Таблицы в базе данных имеют следюющую структуру:
clients (
      c_id BIGINT PRIMARY KEY,
      stateRegPrimeNum BIGINT,
      okved VARCHAR(128),
      okvedNum VARCHAR(10),
      stateRegDt BIGINT);
      
      contacts (
      c_id BIGINT,
      fio VARCHAR(64),
      phone VARCHAR(128),
      is_primary BOOLEAN,
      CONSTRAINT c_id_fk FOREIGN KEY (c_id) REFERENCES clients (c_id));
      
      addrs (
      c_id BIGINT,
      regionID BIGINT,
      addrType VARCHAR(16),
      country VARCHAR(32),
      region VARCHAR(32),
      city VARCHAR(32),
      kladrCode BIGINT,
      CONSTRAINT c_id_fk FOREIGN KEY (c_id) REFERENCES clients (c_id));
      '''

class addr:
   regionID = -1
   addrType = "-"
   country = "-"
   region = "-"
   city = "-"
   kladrCode = -1

class client:
   c_id = -1
   stateRegPrimeNum = -1
   fio = "-"
   phoneNums = []
   addrs = []
   okved = "-"
   okvedNum = "-"
   stateRegDt = -1

def get_phone_list_from_xml(elem):
   phone_list = []
   for phoneNum in elem:
      new_number = 0
      is_primary = False
      for number in phoneNum:
         if (number.tag == "Phone"):
            new_number = number.text
         else:
            if (number.text == "true"):
               is_primary = True
            else:
               is_primary = False
      phone_list.append((new_number, is_primary))
   return(phone_list)

def get_addr_list_from_xml(elem):
   addr_list = []
   for postAddr in elem:
      new_addr = addr()
      for addr_part in postAddr:
         match addr_part.tag:
            case "RegionID":
               new_addr.regionID = int(addr_part.text)
            case "AddrType":
               new_addr.addrType = addr_part.text
            case "Country":
               new_addr.country = addr_part.text
            case "Region":
               new_addr.region = addr_part.text
            case "City":
               new_addr.city = addr_part.text
            case "KladrCode":
               new_addr.kladrCode = int(addr_part.text) if addr_part.text else -1
            case default:
               pass
      addr_list.append(new_addr)
   return(addr_list)

def get_contact_from_xml(elem):
   fio = ""
   phone_list = []
   addr_list = []
   for subelem in elem:
      match subelem.tag:
         case "Card":
            fio = subelem[0].text
         case "PhoneNums":
            phone_list = get_phone_list_from_xml(subelem)
         case "PostAddrs":
            addr_list = get_addr_list_from_xml(subelem)
         case default:
            pass
   return fio, phone_list, addr_list

def get_client_from_XML(path):
   tree = ET.parse(path)
   root = tree.getroot()
   new_client = client()
   for elem in root:
      match elem.tag:
         case "C_ID":
            new_client.c_id = int(elem.text)
         case "StateRegPrimeNum":
            new_client.stateRegPrimeNum = int(elem.text)
         case "OKVED":
            new_client.okved = elem.text
         case "OKVED.NUM":
            new_client.okvedNum = elem.text
         case "ContactInfo":
            new_client.fio, new_client.phoneNums, new_client.addrs = get_contact_from_xml(elem)
         case "StateRegDt":
            new_client.stateRegDt = int(elem[0].text) * 10000 + int(elem[1].text) * 100 + int(elem[2].text)
         case default:
            pass
   return(new_client)

def get_clients_tuple(client):
   return((client.c_id, client.stateRegPrimeNum, client.okved, client.okvedNum, client.stateRegDt))

def get_contact_rows(client):
   rows = []
   for number in client.phoneNums:
      new_phone = (client.c_id, client.fio, number[0], number[1])
      rows.append(new_phone)
   return(rows)

def get_addr_rows(client):
   rows = []
   for addr in client.addrs:
      new_addr = (client.c_id, addr.regionID, addr.addrType, addr.country, 
                  addr.region, addr.city, addr.kladrCode)
      rows.append(new_addr)
   return(rows)

def add_to_tables(cursor, client):
    cursor.execute('''INSERT INTO clients (c_id, stateRegPrimeNum, okved, okvedNum, stateRegDt) 
                    VALUES (%s, %s, %s, %s, %s)''', get_clients_tuple(client))
    conn.commit()
    
    contact_row = get_contact_rows(client)

    for contact_tuple in contact_row:
        cursor.execute('''INSERT INTO contacts (c_id, fio, phone, is_primary) 
                    VALUES (%s, %s, %s, %s)''', contact_tuple)
        conn.commit()

    addr_row = get_addr_rows(client)

    for addr_tuple in addr_row:
        cursor.execute('''INSERT INTO addrs (c_id, regionID, addrType, country, region, city, kladrCode) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)''', addr_tuple)
        conn.commit()


conn = psycopg2.connect(database="postgres",
                        host="localhost",
                        user="postgres",
                        password="",
                        port="8080")

cursor = conn.cursor()

add_to_tables(cursor, get_client_from_XML('testXML.xml'))

conn.close()