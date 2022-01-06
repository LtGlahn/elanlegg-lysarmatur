from json.encoder import JSONEncoder
import pdb
import json
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely import wkt, wkb 
from shapely.geometry import LineString 

from nvdbapiv3 import esriSikkerTekst, nvdbFagdata,  nvdbfagdata2records
# import nvdbgeotricks 

    # nvdbgeotricks.records2gpkg( nvdbfagdata2records( alleElanlegg,     geometri=True), filnavn, 'elanlegg' )
    # nvdbgeotricks.records2gpkg( nvdbfagdata2records( alleLysArmaturer, geometri=True), filnavn, 'lysarmatur' )


def finnLysarmatur( relasjonstre, egenskaper=None ): 
    """
    Rekursiv funksjon som finner alle lysarmatur-objekter i et relasjonstre.

    Inputdata er "barn" - elementet i relasjonstreet. 

    I tillegg kan man velge å føye til egenskaper med nøkkelordet egenskaper = Liste 
    med egenskaper (formattert slik du får fra NVDB api)

    Returnerer liste med NVDB-objekter 
    """
    returListe = [] 

    for enRelasjonstype in relasjonstre: 
        for etObjekt in enRelasjonstype['vegobjekter']: 
            # Sjekker hvert enkelt objekt og ser om det er en lysarmatur
            if isinstance( etObjekt, dict) and 'metadata' in etObjekt and etObjekt['metadata']['type']['navn'] == 'Lysarmatur':

                # Føyer til egenskaper som er oppgitt
                if egenskaper: 
                    etObjekt['egenskaper'].extend( egenskaper )

                returListe.append( etObjekt )

            else: 
            # Traverserer rekursivt alle barn som dette objektet måtte ha
                if isinstance( etObjekt, dict) and 'relasjoner' in etObjekt and 'barn' in etObjekt['relasjoner']:

                    returListe.extend(  finnLysarmatur( etObjekt['relasjoner']['barn'], egenskaper=egenskaper ) )

    return returListe

alleElanlegg = []
alleLysArmaturer = []

def byttKolonneNavn( myDf ): 
    """
    Bytter ut spesialtegn i kolonnenanvn med esriSikkerTekst 
    """

    skiftUt = { }

    for col in list( myDf.columns): 
        nyCol = esriSikkerTekst( col)
        if nyCol != col: 
            skiftUt[col] = nyCol

    myDf.rename( columns=skiftUt, inplace=True )
    return myDf 

if __name__ == '__main__': 

    t0 = datetime.now()
    minCRS = 4326

    elsok = nvdbFagdata( 461 )
    # elsok.filter( { 'kartutsnitt' : '129068.662,6819071.488,307292.352,6909072.335' }) # Stort kartutsnitt
    # elsok.filter( { 'kartutsnitt' : '198660.802,6752983.43,262372.596,6784775.827' })
    # elsok.filter( { 'kartutsnitt' : '231915.469,6754412.201,232089.515,6754500.093' }) # Bitteliten flekk med 1 anlegg 
    elsok.filter( { 'kommune' : 3048  })
    elsok.filter( { 'srid' : minCRS  })

    elsok.statistikk()
    
    counter = 0
    for elAnleggTreff in elsok: 

        counter += 1
        if counter in [1, 5, 10, 50, 100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 7500 ]: 
            print( f"Henter objekt {counter} av {elsok.antall} ")

        try: 
            elanlegg = elsok.forbindelse.les( elAnleggTreff['href'], params={ 'dybde' : 4, 'inkluder' : 'alle'  }  ).json()
        except ValueError:
            pass
        else: 

            # Finner ut om vi har morobjekt tunnelløp? 
            if 'relasjoner' in elanlegg and 'foreldre' in elanlegg['relasjoner']: 
                temp = [ x for x in elanlegg['relasjoner']['foreldre'] if x['type']['id'] == 67 ]

                if len( temp ) >= 1: 

                    elanlegg['egenskaper'].append( { 'id' : -1, 
                                                    'egenskapstype' : 'Tekst',
                                                    'navn' : 'I tunnel', 
                                                    'verdi' : 'JA'  } )
                    elanlegg['egenskaper'].append( { 'id' : -2, 
                                                    'egenskapstype' : 'Heltall',
                                                    'navn' : 'Tunnelløp ID', 
                                                    'verdi' : temp[0]['vegobjekter'][0] } )


            # plukker ut de egenskapene vi ønsker å sende til lysarmaturer
            vilha = [ 'Målernummer', 'MålepunktID', 'Bruksområde' ]
            tmp = [ x for x in elanlegg['egenskaper'] if x['navn'] in vilha ]
            arveEgenskaper = [ ]
            for enEg in tmp: 
                enEg['navn'] = 'ElAnlegg_' + enEg['navn']
                arveEgenskaper.append( enEg )

            if 'geometri' in elanlegg and 'egenskaper' in elanlegg and len( elanlegg['egenskaper'] ) > 0: 
                arveEgenskaper.append( { 'id' : -3, 'navn' : 'ElAnlegg_nvdbId',  'verdi' : elanlegg['id'],               'egenskapstype' : 'Heltall'  } )
                arveEgenskaper.append( { 'id' : -4, 'navn' : 'ElAnlegg_geom',    'verdi' : elanlegg['geometri']['wkt'],  'egenskapstype' : 'Tekst'  } )

                # Finner evt lysarmaturer 
                # Mulige relasjoner: 
                #       elAnlegg => bel.strekning => bel.punkt => lysarmatur
                #       elAnlegg => bel.punkt => lysarmatur
                # Også mulig å hekte bel.punkt på tunnelløp, bygning, rømningslysstrekning.
                # Bruker en rekursiv funksjon som traverserer relasjonstreet og samler opp alle armaturer den finner i en liste
                lysarmaturer = []
                if 'relasjoner' in elanlegg and 'barn' in elanlegg['relasjoner']: 
                    mineLys =  finnLysarmatur( elanlegg['relasjoner']['barn'], egenskaper=arveEgenskaper )
                    mineLysDf = pd.DataFrame( nvdbfagdata2records( mineLys, vegsegmenter=False ))
                    # Summer og aggregerer! 
                    elanlegg['egenskaper'].append( { 'id' : -5, 'navn' : 'Antall NVDB-objekter lysarmatur', 'verdi' : len( mineLys ),       'egenskapstype' : 'Heltall' }   )
                    if len( mineLysDf ) > 0 and 'Effekt' in mineLysDf.columns: 
                        elanlegg['egenskaper'].append( { 'id' : -6, 'navn' : 'Samlet effekt, lysarmatur', 'verdi' : mineLysDf['Effekt'].sum(),  'egenskapstype' : 'Flyttall' }   )

                    lysarmaturer.extend( mineLys ) 

                # Lagrer til mellomresultater
                alleElanlegg.append( elanlegg )
                alleLysArmaturer.extend( lysarmaturer )
            else: 
                print( 'ubrukelig elanlegg-objekt:', json.dumps( elanlegg, indent=4))

    # Knar på elanlegg-data
    eldf  = byttKolonneNavn( pd.DataFrame( nvdbfagdata2records( alleElanlegg,     vegsegmenter=False, geometri=True )) )
    eldf['vegkartlenke'] = 'https://vegkart.atlas.vegvesen.no/#valgt:' + eldf['nvdbId'].astype(str) + ':' + eldf['objekttype'].astype(str)
    eldf.drop( columns=['vegsegmenter', 'relasjoner'], inplace=True )
    lysdf = byttKolonneNavn( pd.DataFrame( nvdbfagdata2records( alleLysArmaturer, vegsegmenter=False, geometri=True )) )
    lysdf['vegkartlenke'] = 'https://vegkart.atlas.vegvesen.no/#valgt:' + lysdf['nvdbId'].astype(str) + ':' + lysdf['objekttype'].astype(str)
    lysdf.drop( columns=['vegsegmenter', 'relasjoner'], inplace=True )
    


    # Lagrer til geopackage 
    filnavn = 'elanlegg_Norge.gpkg'
    eldf['geometry'] = eldf['geometri'].apply( lambda x : wkt.loads( x ))
    elGdf = gpd.GeoDataFrame(  eldf, geometry='geometry', crs=minCRS  )
    # elGdf.to_file( filnavn, layer='elanlegg', driver='GPKG')

    lysdf['geometry'] = lysdf['geometri'].apply( lambda x : wkt.loads( x ))
    lysGdf = gpd.GeoDataFrame(  lysdf, geometry='geometry', crs=minCRS  )
    # lysGdf.to_file( filnavn, layer='lysarmatur', driver='GPKG')


    # nvdbgeotricks.records2gpkg( nvdbfagdata2records( alleElanlegg,     geometri=True), filnavn, 'elanlegg' )
    # nvdbgeotricks.records2gpkg( nvdbfagdata2records( alleLysArmaturer, geometri=True), filnavn, 'lysarmatur' )

    # Lager fancy kartvisning med linje fra lysarmatur => El.anlegg
    # For å tvinge 2D-geometri bruker vi tricks med wkb.loads( wkb.dumps( GEOM, output_dimension=2 ))
    lysdf['geometry'] = lysdf.apply( lambda x: LineString( [ wkb.loads( wkb.dumps( wkt.loads( x['geometri']),      output_dimension=2 )), 
                                                             wkb.loads( wkb.dumps( wkt.loads( x['ElAnlegg_geom']), output_dimension=2 ))  ] 
                                    ) , axis=1)

    minGdf = gpd.GeoDataFrame( lysdf, geometry='geometry', crs=minCRS )       
    # må droppe kolonne vegsegmenter hvis data er hentet med vegsegmenter=False 
    if 'vegsegmenter' in minGdf.columns:
        minGdf.drop( 'vegsegmenter', 1, inplace=True)
    if 'relasjoner' in minGdf.columns:
        minGdf.drop( 'relasjoner', 1, inplace=True)
    
    minGdf.to_file( filnavn, layer='kartvisning_lysarmatur', driver="GPKG")  

    #  Lagrer til excel 
    with pd.ExcelWriter( 'elanlegg_lysarmatur_Norge.xlsx') as writer: 

        # Fjerner geometrikolonner fra excel 
        col_eldf = [ x for x in list( eldf.columns)  if not 'geom' in x.lower()  ]
        col_lys =  [ x for x in list( lysdf.columns) if not 'geom' in x.lower() ]
        eldf[ col_eldf ].to_excel( writer, sheet_name='Elektrisk anlegg',  index=False )
        lysdf[ col_lys ].to_excel( writer, sheet_name='Lysarmatur',        index=False )


    dT = datetime.now() - t0
    print( f"Tidsbruk: { round( dT.total_seconds(), 1)} sekunder" )