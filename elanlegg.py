from json.encoder import JSONEncoder
import pdb
import json
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely import wkt, wkb 
from shapely.geometry import LineString 

import lokal_STARTHER
import nvdbapiv3
import nvdbgeotricks 

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

if __name__ == '__main__': 
    t0 = datetime.now()

    elsok = nvdbapiv3.nvdbFagdata( 461 )
    mittfilter = {}
    # mittfilter =  { 'kartutsnitt' : '276891.64,6654048.52,280547.83,6656183.06' } # Debug Kjeller skole 
    # mittfilter =  { 'kartutsnitt' : '129068.662,6819071.488,307292.352,6909072.335' } # Stort kartutsnitt
    # mittfilter =  { 'kartutsnitt' : '198660.802,6752983.43,262372.596,6784775.827' }
    # mittfilter =  { 'kartutsnitt' : '231915.469,6754412.201,232089.515,6754500.093' } # Bitteliten flekk med 1 anlegg 
    # mittfilter =   { 'kommune' : 3048  }) 
    elsok.filter( mittfilter)

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
                    mineLysDf = pd.DataFrame( nvdbapiv3.nvdbfagdata2records( mineLys, vegsegmenter=False ))
                    # Summer og aggregerer! 
                    elanlegg['egenskaper'].append( { 'id' : -5, 'navn' : 'Antall NVDB-objekter lysarmatur', 'verdi' : len( mineLys ),       'egenskapstype' : 'Heltall' }   )
                    if len( mineLysDf ) > 0 and 'Effekt' in mineLysDf.columns: 
                        elanlegg['egenskaper'].append( { 'id' : -6, 'navn' : 'Samlet effekt, lysarmatur', 'verdi' : mineLysDf['Effekt'].sum(),  'egenskapstype' : 'Flyttall' }   )

                    lysarmaturer.extend( mineLys ) 

                # Legger på fylke og kontraktsområde
                elanlegg['egenskaper'].append( { 'id' : -7, 'navn' :  'Elanlegg_fylke', 'verdi' : elanlegg['lokasjon']['fylker'][0], 'egenskapstype' : 'Heltall' } )
                elanlegg['egenskaper'].append( { 'id' : -8, 'navn' :  'Elanlegg_kontraktsomr', 
                                                'verdi' : ','.join( [ x['navn'] for x in elanlegg['lokasjon']['kontraktsområder'] ] ), 'egenskapstype' : 'Tekst' } )

                # Lagrer til mellomresultater
                alleElanlegg.append( elanlegg )
                alleLysArmaturer.extend( lysarmaturer )
            else: 
                print( 'ubrukelig elanlegg-objekt:', json.dumps( elanlegg, indent=4))

    t1 = datetime.now( ) - t0
    print( f"Tidsbruk analyse av el.anlegg relasjonstrær: {t1}"  )

    # Knar på elanlegg-data
    # Eget lag: Linje som viser kobling mellom armatur og elektrisk anlegg (du har med)
    eldf  = pd.DataFrame( nvdbapiv3.nvdbfagdata2records( alleElanlegg,     vegsegmenter=False, geometri=True ))
    eldf['vegkartlenke'] = 'https://vegkart.atlas.vegvesen.no/#valgt:' + eldf['nvdbId'].astype(str) + ':' + eldf['objekttype'].astype(str)
    eldf.drop( columns=['vegsegmenter', 'relasjoner'], inplace=True )
    lysdf = pd.DataFrame( nvdbapiv3.nvdbfagdata2records( alleLysArmaturer, vegsegmenter=False, geometri=True ))
    lysdf['vegkartlenke'] = 'https://vegkart.atlas.vegvesen.no/#valgt:' + lysdf['nvdbId'].astype(str) + ':' + lysdf['objekttype'].astype(str)
    lysdf.drop( columns=['vegsegmenter', 'relasjoner'], inplace=True )



    # Eget lag: Belysningsstrekningen uten lysarmatur (Ny)

    # Eget lag: Lysarmatur som ikke har kobling til elektrisk anlegg gjerne med informasjon om hva som mangler 
    # tilsvarende skjermdumpen nedenfor (Ny)
    # Henter alle lysarmaturer (med evt filter) 
    t2 = datetime.now()
    print( "Henter alle lysarmaturer")
    HeleNVDBLysarmatur = nvdbapiv3.nvdbFagdata( 88, filter=mittfilter).to_records( relasjoner=True, geometri=True, vegsegmenter=False  )

    # Henter alle belysningspunkt 
    print( "Henter alle belysningspunkt")
    HeleNVDBBelpunkt = pd.DataFrame( nvdbapiv3.nvdbFagdata( 87, filter=mittfilter ).to_records( relasjoner=True, geometri=True, vegsegmenter=False  ))

    # Henter alle belysningsstrekninger
    print( "Henter alle belysningsstrekninger") 
    HeleNVDBBelStrekning = pd.DataFrame( nvdbapiv3.nvdbFagdata( 86, filter=mittfilter ).to_records( 
                                                        relasjoner=True, geometri=False, vegsegmenter=False ))


    t3 = datetime.now()
    print(f"Tidsbruk nedlasting alle lysarmaturer, bel.punkt og bel.strekninger: {t3-t2}")
    # Fjerner de lysarmaturene som vi vet om allerede (dvs der vi kjenner relasjon el.anlegg->lysarmatur 
    # manglerMor = HELENVDBLysarmatur[ ~HELENVDBLysarmatur['nvdbId'].isin( lysdf['nvdbId']) ]
    # Tygger oss gjennom relasjoner oppover fra lysarmatur
    lysarmatur_mangler_elanlegg = []
    lysdf_nvdbId = lysdf['nvdbId'].to_list()
    for armatur in HeleNVDBLysarmatur: 
        if armatur['nvdbId'] not in lysdf_nvdbId: # Hopper over denna her hvis vi fant lysarmaturen i de relasjonstreene vi har analysert
            
            lysarmatur_relasjonsvurdering = 'ERROR - analyse av relasjoner feilet'
            # Tygger oss gjennom relasjoner: 
            if not 'relasjoner' in armatur or not 'foreldre' in armatur['relasjoner'] or len( armatur['relasjoner']['foreldre']) == 0: 
                lysarmatur_relasjonsvurdering = 'Lysarmatur mangler foreldre'
            elif len( armatur['relasjoner']['foreldre']) > 1: 
                lysarmatur_relasjonsvurdering = f"Lysarmatur har {len( armatur['relasjoner']['foreldre'])} foreldrerelasjoner"
            elif armatur['relasjoner']['foreldre'][0]['type']['navn'] == 'Belysningspunkt': 
                # Analyserer relasjon fra bel.punkt og oppover 
                tempBelPunkt = HeleNVDBBelpunkt[ HeleNVDBBelpunkt['nvdbId'] == armatur['relasjoner']['foreldre'][0]['vegobjekter'][0] ]

                if len( tempBelPunkt ) == 1: 
                    tempBelPunkt = tempBelPunkt.iloc[0]

                    if isinstance( tempBelPunkt['relasjoner'], float) or not 'foreldre' in tempBelPunkt['relasjoner'] or len( tempBelPunkt['relasjoner']['foreldre'] ) == 0: 
                        lysarmatur_relasjonsvurdering = 'Lysarmatur si mor er et bel.punkt uten foreldre'
                    elif len( armatur['relasjoner']['foreldre']) > 1: 
                        lysarmatur_relasjonsvurdering = f"Lysarmatur si mor er et bel.punkt med {len( tempBelPunkt['relasjoner']['foreldre'])} foreldrerelasjoner"
                    elif tempBelPunkt['relasjoner']['foreldre'][0]['type']['navn'] == 'Belysningsstrekning': 
                        # Analyser relasjoner fra belysningsstrekning og oppover 

                        tempBelStrek = HeleNVDBBelStrekning[ HeleNVDBBelStrekning['nvdbId'] == tempBelPunkt['relasjoner']['foreldre'][0]['vegobjekter'][0] ] 

                        if len( tempBelStrek ) == 1: 
                            tempBelStrek = tempBelStrek.iloc[0]

                            if isinstance( tempBelStrek['relasjoner'], float) or not 'foreldre' in tempBelStrek['relasjoner'] or len( tempBelStrek['relasjoner']['foreldre']) == 0: 
                                lysarmatur_relasjonsvurdering = 'Belysningsstrekning mangler foreldre'
                            elif  len( tempBelStrek['relasjoner']['foreldre']) > 1: 
                                lysarmatur_relasjonsvurdering = f"Lysarmatur si mor er et bel.punkt med {len( tempBelStrek['relasjoner']['foreldre'])} foreldrerelasjoner"

                            else: 

                                lysarmatur_relasjonsvurdering = 'Relasjon ' 
                                lysarmatur_relasjonsvurdering += str(  tempBelStrek['relasjoner']['foreldre'][0]['type']['id'] ) 
                                lysarmatur_relasjonsvurdering += ' ' 
                                lysarmatur_relasjonsvurdering += tempBelStrek['relasjoner']['foreldre'][0]['type']['navn']
                                lysarmatur_relasjonsvurdering += ' => 86 Belysningsstrekning => 87 Belysningspunkt => 88 Lysarmatur'

                        else: 
                            lysarmatur_relasjonsvurdering = 'Trøbbel med å finne mor-objekt til belysningspunkt'


                    else: 
                        lysarmatur_relasjonsvurdering = 'Relasjon ' 
                        lysarmatur_relasjonsvurdering += str(  tempBelPunkt['relasjoner']['foreldre'][0]['type']['id'] ) 
                        lysarmatur_relasjonsvurdering += ' ' 
                        lysarmatur_relasjonsvurdering += tempBelPunkt['relasjoner']['foreldre'][0]['type']['navn']
                        lysarmatur_relasjonsvurdering += '=> 87 Belysningspunkt => 88 Lysarmatur'

            else: 
                lysarmatur_relasjonsvurdering =  'Lysarmatur foreldrerelasjon:' + str(armatur['relasjoner']['foreldre'][0]['id']) + ' ' + armatur['relasjoner']['foreldre'][0]['type'] 

            armatur['Lysarmatur_relasjonsvurdering'] =  lysarmatur_relasjonsvurdering 
            lysarmatur_mangler_elanlegg.append( armatur )

    lysarmatur_mangler_elanlegg = pd.DataFrame( lysarmatur_mangler_elanlegg )
    lysarmatur_mangler_elanlegg['geometry'] = lysarmatur_mangler_elanlegg['geometri'].apply( wkt.loads )
    lysarmatur_mangler_elanlegg = gpd.GeoDataFrame( lysarmatur_mangler_elanlegg, geometry='geometry', crs=5973 )

    # Lagrer til geopackage 
    mappenavn = '/var/www/html/nvdbdata/elanlegg-lysarmatur/'
    # mappenavn = ''
    filnavn = mappenavn + 'elanlegg_norge.gpkg'

    eldf['geometry'] = eldf['geometri'].apply( lambda x : wkt.loads( x ))
    elGdf = gpd.GeoDataFrame(  eldf, geometry='geometry', crs=5973  )
    elGdf.to_file( filnavn, layer='elanlegg', driver='GPKG')

    lysdf['geometry'] = lysdf['geometri'].apply( lambda x : wkt.loads( x ))
    lysGdf = gpd.GeoDataFrame(  lysdf, geometry='geometry', crs=5973  )
    lysGdf.to_file( filnavn, layer='lysarmatur', driver='GPKG')

    # Lager fancy kartvisning med linje fra lysarmatur => El.anlegg
    # For å tvinge 2D-geometri bruker vi tricks med wkb.loads( wkb.dumps( GEOM, output_dimension=2 ))
    lysdf['geometry'] = lysdf.apply( lambda x: LineString( [ wkb.loads( wkb.dumps( wkt.loads( x['geometri']),      output_dimension=2 )), 
                                                             wkb.loads( wkb.dumps( wkt.loads( x['ElAnlegg_geom']), output_dimension=2 ))  ] 
                                    ) , axis=1)

    minGdf = gpd.GeoDataFrame( lysdf, geometry='geometry', crs=5973 )       
    # må droppe kolonne vegsegmenter hvis data er hentet med vegsegmenter=False 
    if 'vegsegmenter' in minGdf.columns:
        minGdf.drop( 'vegsegmenter', 1, inplace=True)
    if 'relasjoner' in minGdf.columns:
        minGdf.drop( 'relasjoner', 1, inplace=True)
    
    minGdf.to_file( filnavn, layer='kartvisning_lysarmatur', driver="GPKG")  

    lysarmatur_mangler_elanlegg.drop( columns='vegsegmenter', inplace=True )
    lysarmatur_mangler_elanlegg.to_file( filnavn, layer='Lysarmatur uten el.anlegg relasjon', driver='GPKG')


    # Eget lag: Elektrisk anlegg med bruksområde «Veglys eller manglende verdi» og måling type «umålt» (NY)
    # Eget lag: Elektrisk anlegg med bruksområde «Veglys eller manglende verdi» og måling type «alle andre enn umålt» (NY)
    # Eget lag: Elektrisk anlegg med bruksområde «Veglys eller manglende verdi» uten lysarmatur (Ny)
    el_veglysGDF = elGdf[ (elGdf['ElAnlegg_Bruksområde'].isnull() ) | (elGdf['ElAnlegg_Bruksområde'] == 'Veglys' )]
    el_veglysGDF[ el_veglysGDF['Måling type'] == 'Umålt'].to_file( filnavn, layer='Veglys anlegg umålt', driver='GPKG')
    el_veglysGDF[ el_veglysGDF['Måling type'] != 'Umålt'].to_file( filnavn, layer='Veglys anlegg IKKE umålt', driver='GPKG')
    elGdf[ elGdf['Antall NVDB-objekter lysarmatur'].isnull()].to_file( filnavn , layer='Veglys anlegg uten lysarmatur')

    # Elektriske anlegg uten lysarmatur
    elGdf[ elGdf['Antall NVDB-objekter lysarmatur'].isnull()].to_file( filnavn, layer='Elektrisk anlegg uten lysarmatur', driver='GPKG')

    print( f"Tidsbruk dataknaing og lagring: {datetime.now()-t3}")

  # Lagrer til excel 
    # with pd.ExcelWriter( 'elanlegg_lysarmatur_Norge.xlsx') as writer: 

    #     # Fjerner geometrikolonner fra excel 
    #     col_eldf = [ x for x in list( eldf.columns)  if not 'geom' in x.lower()  ]
    #     col_lys =  [ x for x in list( lysdf.columns) if not 'geom' in x.lower() ]
    #     eldf[ col_eldf ].to_excel( writer, sheet_name='Elektrisk anlegg',  index=False )
    #     lysdf[ col_lys ].to_excel( writer, sheet_name='Lysarmatur',        index=False )

