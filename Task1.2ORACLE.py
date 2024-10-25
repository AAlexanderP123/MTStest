import xml.etree.ElementTree as ET
import oracledb

'''Таблицы в базе данных имеют следюющую структуру:
clients (
      c_id NUMBER PRIMARY KEY,
      stateRegPrimeNum NUMBER,
      okved VARCHAR(128),
      okvedNum VARCHAR(10),
      stateRegDt NUMBER);
      
      contacts (
      c_id NUMBER,
      fio VARCHAR(64),
      phone VARCHAR(128),
      is_primary BOOLEAN,
      CONSTRAINT c_id_fk FOREIGN KEY (c_id) REFERENCES clients (c_id));
      
      addrs (
      c_id NUMBER,
      regionID NUMBER,
      addrType VARCHAR(16),
      country VARCHAR(32),
      region VARCHAR(32),
      city VARCHAR(32),
      kladrCode NUMBER,
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

def get_clients_row(client):
   rows = [
      {"c_id":client.c_id, "stateRegPrimeNum":client.stateRegPrimeNum, "okved":client.okved, "okvedNum":client.okvedNum, "stateRegDt":client.stateRegDt}
   ]
   return(rows)

def get_contact_rows(client):
   rows = []
   for number in client.phoneNums:
      new_phone = {"c_id":client.c_id, "fio":client.fio, "phone":number[0], "is_primary":number[1]}
      rows.append(new_phone)
   return(rows)

def get_addr_rows(client):
   rows = []
   for addr in client.addrs:
      new_addr = {"c_id":client.c_id, "regionID":addr.regionID, "addrType":addr.addrType, "country":addr.country, 
                  "region":addr.region, "city":addr.city, "kladrCode":addr.kladrCode}
      rows.append(new_addr)
   return(rows)

def add_to_tables(cursor, client):

   cursor.executemany("""INSERT INTO clients(c_id, stateRegPrimeNum, okved, okvedNum, stateRegDt) 
                      VALUES (:c_id, :stateRegPrimeNum, :okved, :okvedNum, :stateRegDt)""", get_clients_row(client))
   cursor.executemany("""INSERT INTO contacts(c_id, fio, phone, is_primary) 
                      VALUES (:c_id, :fio, :phone, :is_primary)""", get_contact_rows(client))
   cursor.executemany("""INSERT INTO addrs(c_id, regionID, addrType, country, region, city, kladrCode) 
                      VALUES (:c_id, :regionID, :addrType, :country, :region, :city, :kladrCode)""", get_addr_rows(client))

connection = oracledb.connect(
   user="username",
   password="password",
   dsn="localhost/xepdb1")

print("Successfully connected to Oracle Database")

cursor = connection.cursor()

add_to_tables(cursor, get_client_from_XML('testXML.xml'))
 