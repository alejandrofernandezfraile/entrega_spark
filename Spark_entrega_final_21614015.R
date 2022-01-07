# MÓDULO II: POSICIONAMIENTO EMPRESARIAL DEL BIG DATA ---------------------

# Práctica de Spark -------------------------------------------------------
#Alejandro Fernández


# Librerías utilizadas ----------------------------------------------------

pacman::p_load(httr, tidyverse,leaflet,janitor,readr,sparklyr, XML, xlsx )
library(sparklyr)
library(dplyr)

# Conectarse a  spark --------------------------------------------------------

sc <- spark_connect(master = "local")

# Obtención de datos desde fuentes veraces --------------------------------

url<-	"https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

httr::GET(url)

# EJERCICIO A --------------------------------

#A. De fuentes veraces, lea los archivos que se indican en el anexo, como podrá apreciar, el/los archivos contiene miles de filas por decenas de columnas; solo es posible tratarlos utilizando Spark si queremos respuestas en tiempo real;

#i.Limpie el/los dataset(s) ( la información debe estar correctamente formateada, por ej. lo que es de tipo texto no debe tener otro tipo que no sea texto) , ponga el formato correcto en los números, etc., etc.
ds <- jsonlite::fromJSON(url)
ds <- ds$ListaEESSPrecio
ds <- ds %>% as_tibble() %>% clean_names() 

ds <- ds  %>% type_convert(locale = locale(decimal_mark = ",")) %>% view() %>% clean_names() 

glimpse(ds)
view(ds)

#iii. cree una columna nueva que deberá llamarse low-cost, y determine cuál es el precio promedio de todos los combustibles a nivel comunidades autónomas, así como para las provincias, tanto para el territorio peninsular e insular, esta columna deberá clasificar las estaciones por lowcost y no lowcost,

colnames(ds)
ds_col_lowcost <- ds %>%mutate(low_cost=rotulo%in%c("REPSOL","CAMPSA","BP", "SHELL","GALP", "CEPSA")) %>% view()

#renombro los elementos de la columna low_cost recien creada
ds_col_lowcost$low_cost[ds_col_lowcost$low_cost == TRUE] <- "no_low_cost"
ds_col_lowcost$low_cost[ds_col_lowcost$low_cost == FALSE] <- "low_cost"

View(ds_col_lowcost)

#calcular la media del precio de todos los combustibles
mean_precios <- ds_col_lowcost %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(idccaa, provincia) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=T), media_precio_biodiesel=mean(precio_biodiesel, na.rm=T), media_precio_gas_natural_comprimido=mean(precio_gas_natural_comprimido, na.rm=T), media_precio_gas_natural_licuado=mean(precio_gas_natural_licuado, na.rm=T), media_precio_gases_licuados_del_petroleo=mean(precio_gases_licuados_del_petroleo, na.rm=T), media_precio_gasoleo_a=mean(precio_gasoleo_a, na.rm=T), media_precio_gasoleo_b=mean(precio_gasoleo_b, na.rm=T), media_precio_gasoleo_premium=mean(precio_gasoleo_premium, na.rm=T), media_precio_gasolina_95_e5=mean(precio_gasolina_95_e5, na.rm=T), media_precio_gasolina_95_e5_premium=mean(precio_gasolina_95_e5_premium, na.rm=T), media_precio_gasolina_98_e10=mean(precio_gasolina_98_e10, na.rm=T), media_precio_gasolina_98_e5=mean(precio_gasolina_98_e5, na.rm=T),media_precio_hidrogen=mean(precio_hidrogeno, na.rm=T)) %>% view()

View(mean_precios)

#Procedo a copiar las tablas recien creadas en spark
copy_to(sc,mean_precios,overwrite = T)
copy_to(sc,ds_col_lowcost,overwrite = T)

#iv. Imprima en un mapa interactivo, la localización del top 10 mas caras y otro mapa interactivo del top 20 mas baratas, estos 2 archivos deben guardarse en formato HTML y pdf para su posterior entrega al inversor., nombre de los archivos : top_10.html, top_10.pdf y top_20.html, top_20.pdf

#Para hacer este ejercicio, dado que no se especifica una gasolina concreta, eligo el "precio_gasolina_95_e10"

# top10 gasolineras mas caras
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e10, localidad, direccion) %>% 
  top_n(10, precio_gasolina_95_e10) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e10) 

# top20 gasolineras mas baratas 
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e10, localidad, direccion) %>% 
  top_n(-20, precio_gasolina_95_e10) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e10 )


#v. conseguidos objetivos anteriores, debe guardar este “archivo” en una nueva tabla llamada low-cost_num_expediente y deberá estar disponible también en su repositorio de Github con el mismo nombre y formato csv.

write.csv(ds_col_lowcost,"Desktop/low_cost_21614015.csv", row.names = FALSE)
write.csv(mean_precios,"Desktop/mean_precios_21614015.csv", row.names = FALSE)


# EJERCICIO B --------------------------------

#B.Este empresario tiene sus residencias habituales en Madrid y Barcelona , por lo que, en principio le gustaría implantarse en cualquiera de las dos antes citadas, y para ello quiere saber :

#i. cuántas gasolineras tiene la comunidad de Madrid y en la comunidad de Cataluña, cuántas son low-cost, cuantas no lo son,

Clasificación_lowcost_MAD_BCN <- ds_col_lowcost %>% select(idccaa, low_cost, provincia) %>% filter(idccaa %in% c("13","09")) %>% 
  group_by(idccaa) %>% count(low_cost)

view(Clasificación_lowcost_MAD_BCN)

#ii. además, necesita saber cuál es el precio promedio, el precio más bajo y el más caro de los siguientes carburantes: gasóleo A, y gasolina 95 e Premium.

max_min_mean_MAD_BCN <- ds_col_lowcost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% drop_na() %>% 
  filter(idccaa %in% c("13","09")) %>% 
  group_by(idccaa, low_cost) %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))

view(max_min_mean_MAD_BCN)

#iii. Conseguido el objetivo, deberá guardar este “archivo” en una nueva tabla llamada informe_MAD_BCN_expediente y deberá estar disponible también en su repositorio con el mismo nombre en formato csv

write.csv(Clasificación_lowcost_MAD_BCN,"Desktop/informe_count_lowcost_MAD__BCN_21614015.csv", row.names = FALSE)

write.csv(max_min_mean_MAD_BCN, "Desktop/informe_MAD_BCN_21614015.csv", row.names = FALSE)

#Procedo a copiar las tablas recien creadas en spark
copy_to(sc,Clasificación_lowcost_MAD_BCN,overwrite = T)
copy_to(sc,max_min_mean_MAD_BCN,overwrite = T)
  

# EJERCICIO C --------------------------------
  
#c. Por si las comunidades de Madrid y Cataluña no se adapta a sus requerimientos, el empresario también quiere :
  
#i. conocer a nivel municipios, cuántas gasolineras son low-cost, cuantas no lo son, cuál es el precio promedio, 
#el precio más bajo y el más caro de los siguientes carburantes: gasóleo A, y gasolina 95 e5 Premium , en todo el TERRITORIO NACIONAL, exceptuando las grandes CIUDADES ESPAÑOLAS ("MADRID", "BARCELONA", "SEVILLA" y "VALENCIA")
  
no_grandes_ciudades <- ds_col_lowcost %>% select(idccaa, id_municipio, municipio, low_cost, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% group_by(municipio, low_cost) %>% filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium)) 

View(no_grandes_ciudades)

count_lowcost_municipios <- no_grandes_ciudades %>% group_by(low_cost) %>%count(low_cost)
View(count_lowcost_municipios) 

#Procedo a copiar las tablas recien creadas en spark
copy_to(sc,no_grandes_ciudades,overwrite = T)
copy_to(sc,count_lowcost_municipios,overwrite = T)

#ii. Conseguido el objetivo, deberá guardar este “archivo” en una nueva tabla llamada 
#informe_no_grandes_ciudades_expediente y deberá estar disponible también en su repositorio con el mismo nombre en formato Excel

write.csv(no_grandes_ciudades,"Desktop/informe_no_grandes_ciudades_21614015.csv")


# EJERCICIO D --------------------------------

#d. i. que gasolineras se encuentran abiertas las 24 horas exclusivamente, genere una nueva 
#tabla llamada no_24_horas sin la variable horario (es decir no debe aparecer esta columna).

colnames(ds)

abiertas <- ds_col_lowcost %>% select(c_p, direccion, localidad, municipio, rotulo, horario) %>% filter(
  horario=="L-D: 24H")

View(abiertas)

no_24horas <- abiertas %>% select(c_p, direccion, localidad, municipio, rotulo, -horario)
View(no_24horas)

#Procedo a copiar la tabla recien creadas en spark
copy_to(sc,no_24horas,overwrite = T)

#ii. Conseguido el objetivo, deberá guardar este “archivo” en una nueva tabla llamada no_24_horas y deberá estar disponible también en su repositorio con el mismo nombre en formato Excel

write.xlsx(no_24horas,"Desktop/no_24h.xls")


# EJERCICIO E --------------------------------

#e. Uno de los factores más importantes paraqueelempresariosedecantea instalar nuevas gasolineras es la demanda que viene dada por la población 
#y la competencia existente en un municipio donde se pretenda implantar las gasolineras, para responder a esta pregunta de negocio,

library(readxl)

#Obtención de datos desde INE(info actualizada 2021)

pobmun21 <- read_excel("Desktop/G- Drive/2.Posicionamiento Big Data/Spark/TAREA SPARK/pobmun21.xlsx")
View(pobmun21)

#i. deberá añadir la población al dataset original creando una nueva columna denominada población, esta información debe ser veraz 
#y la más actualizada, la población debe estar a nivel municipal ( todo el territorio nacional)

#limpieza del dataset
names(pobmun21) = c("id_provincia", "provincia", "cnum", "municipio", "poblacion")
View(pobmun21)

#Procedo a juntar los datasets
View(ds)

ds_poblacion<-left_join(x=ds, y=pobmun21, by="municipio")
View(ds_poblacion)

#Procedo a copiar la tabla recien creadas en spark
copy_to(sc,ds_poblacion,overwrite = T)

#ii. este empresario ha visto varios sitios donde potencialmente le gustaría instalar su gasolinera, eso sitios están representados por la dirección, 
#desde esta ultima calcule cuanta competencia ( nombre de la gasolinera y direccion) tiene en :

#1. En un radio de 1 km ( genere mapa_competencia1.html) 
#2. En un radio de 2 km ( genere mapa_competencia2.html) 
#3. En un radio de 4 km ( genere mapa_competencia3.html)

ds_poblacion %>%  select(rotulo, latitud, longitud_wgs84, direccion, municipio, provincia.y) %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~provincia.y)

#1
ds_poblacion %>%  select(rotulo, latitud, longitud_wgs84, direccion, municipio, provincia.y) %>% 
  filter(provincia.y=='Madrid') %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~provincia.y) %>% addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 1000)
#2
ds_poblacion %>%  select(rotulo, latitud, longitud_wgs84, direccion, municipio, provincia.x) %>% 
  filter(municipio=='San Sebastián de los Reyes') %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~municipio) %>% addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 2000)

#3
ds_poblacion %>%  select(rotulo, latitud, longitud_wgs84, direccion, municipio, provincia.x) %>% 
  filter(municipio=='Mula') %>% 
  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~municipio) %>% addCircles(lng = ~longitud_wgs84, lat = ~latitud, radius = 4000)

#Procedo a copiar la tabla recien creadas en spark
copy_to(sc,ds_poblacion,overwrite = T)


#iii. genere el TopTen de municipios entre todo el territorio nacional excepto el territorio insular, 
#donde no existan gasolineras 24 horas, agrupadas entre low-cost y no low-cost, 
#deberá guardar este “archivo” en una nueva tabla llamada informe_top_ten_expediente y 
#deberá estar disponible también en su repositorio con el mismo nombre en formato csv.

gasolineras_municipio_24h<- ds_col_lowcost %>% filter (!provincia %in% c("BALEARS (ILLES)", "PALMAS (LAS)")) %>% 
  filter (!horario=="L-D: 24H" )  %>% group_by (municipio, low_cost) %>% count()  

 View(gasolineras_municipio_24h)

informe_top_ten_21614015 <- gasolineras_municipio_24h[order(gasolineras_municipio_24h$n, decreasing = TRUE),] %>% head(10) 
view(informe_top_ten_21614015)

#Guardo en formato csv
write.csv(informe_top_ten_21614015,"Desktop/informe_top_ten_21614015.csv")

#Procedo a copiar la tabla recien creadas en spark
copy_to(sc,informe_top_ten_21614015,overwrite = T)


# ver los objetos de spark ------------------------------------------------
  src_tbls(sc)


#ALEJANDRO FERNANDEZ

