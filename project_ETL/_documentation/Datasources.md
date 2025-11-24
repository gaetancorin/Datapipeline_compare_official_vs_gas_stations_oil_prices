# DataSources
This section provides an explanation of each data source used in the project.

## Gas Station Data
This dataset contains minute-by-minute records of fuel price changes from French gas stations.  

You can find the official documentation here:  
https://www.prix-carburants.gouv.fr/rubrique/opendata/

In this project, the data is retrieved via the following endpoint:
https://donnees.roulez-eco.fr/opendata/annee/{year}

Example for the year 2008:  
[https://donnees.roulez-eco.fr/opendata/annee/2008](https://donnees.roulez-eco.fr/opendata/annee/2008)

### Data Details
- **Format:** XML (one file per year)  
- **Available years:** 2007 → present  
- **Fuel types included:**  
  - Gazole  
  - SP95  
  - SP98  
  - E10  
  - E85  
  - GPLc  
- **Unit:** Euros per liter (€/L)  
- **Taxes:** All prices include VAT (TTC)


## Official Government Data
This dataset represents **denormalized monthly average fuel prices** calculated by the French government.  

The dataset can be accessed here:  
https://www.ecologie.gouv.fr/politiques-publiques/prix-produits-petroliers

To access the data, you need to fill out the form **'Base de données des prix des carburants et combustibles en France: Démarrer'**. 

Submitting the form provides a **dynamic URL** to download the CSV file.  

Example of a dynamic URL:  
[https://www.ecologie.gouv.fr/simulator-energies/monitoring/export/edda41d9d3219ffaa0300e846b92aff1b36aa81a](https://www.ecologie.gouv.fr/simulator-energies/monitoring/export/edda41d9d3219ffaa0300e846b92aff1b36aa81a)

### Data Details
- **Format:** CSV (single file containing all available years)  
- **Available years:** 1985 → present  
- **Fuel types included:**  
  - Gazole  
  - SP95  
  - SP98  
  - E10  
  - E85  
  - GPLc  
- **Unit:** Euros per liter (€/L)  
- **Taxes:** All prices include VAT (TTC)
