# esri-ArcPy-logisticsBackEnd
Geoprocessing script that analyzes user input to determine all possibilities for utilizing a logistics system to complete delivery by a specific date. 

Built on a custom data model that, frankly, needs improvement. The script accepts and address, a number and type of products, and a due date and returns a range of possiblities (if they exist) on how that product can be passed through the intersecting logistics system(s) for delivery. 

Live application where this GP is utlized (widget is in UR of UI): https://mapwv.gov/crc/

Associated geoprocessing service: https://appservices.wvgis.wvu.edu/arcgis/rest/services/CRC/CRCTool/GPServer/turnrow%20logistics%20sharing%20tool/

Associated front end built as a widget for an ESRI ArcGIS Web AppBuilder application here:  https://github.com/erghjunk/esri-WAP-widget-logisticsFrontEnd/
