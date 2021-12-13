import configuration as config
import pandas as pd
from datetime import datetime
from queryTable import QueryTable


class IntramuralLocations(QueryTable):
    """
    Object based on dataframe which contains:

    | kamerId | kamerNaam | afdelingId | afdelingNaam | locationId | locatieNaam |
    """
    query = """
        select 
            afdeling.objectId as kamerId,
            afdeling.name as kamerNaam,
            afdeling.intramuralLocation,
            r.description as roomType,
            afdeling.afdelingId,
            afdeling.afdelingNaam,
            afdeling.locationId,
            locations.name as locatieNaam 
        from locations
        join (
            select
                kamers.*,
                locations.objectId as afdelingId,
                locations.name as afdelingNaam,
                locations.parentObjectId as locationId
            from locations
            right join (
                select * from locations
                where type = 1
            ) as kamers on kamers.parentObjectId = locations.objectId
        ) as afdeling on afdeling.locationId = locations.objectId
        left join roomtypes r on r.objectId = afdeling.roomTypeObjectId
        """

    def __init__(self):
        super().__init__(query=self.query)
        self.kamers = self.__clean_hoge_prins_willemhof(self.dataframe)
        self.kamers = self.__clean_renting(self.kamers, 'kamerId')
        self.dataframe = self.kamers

        self.afdelingen = self.kamers[[
            'afdelingId', 'afdelingNaam', 'locationId', 'locatieNaam']].drop_duplicates()
        self.afdelingen = self.__clean_renting(self.afdelingen, 'afdelingId')

        self.locaties = self.afdelingen[[
            'locationId', 'locatieNaam']].drop_duplicates()
        self.locaties = self.__clean_renting(self.locaties, 'locationId')

    def __clean_hoge_prins_willemhof(self, location_df):
        # location Hoge Prins Willemhof has only two location layers
        # therefore afdeling and locatie has to be named the same given context
        respect = location_df['locationId'] == 1
        location_df.loc[respect, 'locationId'] = 464
        location_df.loc[respect, 'locatieNaam'] = 'Hoge Prins Willemhof'
        return location_df

    def __clean_renting(self, location_df, col):
        # print(location_df[col].unique())
        rentingIds = set(RentingLocations().dataframe['locationId'])
        # print(rentingIds)
        isRenting = location_df[col].isin(rentingIds)
        location_df = location_df[~isRenting]
        # print(location_df[col].unique())
        return location_df


class ExtramuralLocations(QueryTable):
    query = """
    select 
        l.objectId as locationId,
        l.name as locatieNaam
    from locations l
    where l.objectId in (
        270, # Renbaankwartier
        98,  # Belgisch Park
        271, # Benoordenhout
        272, # Scheveningen Dorp
        273  # Duindorp
    )
    """

    def __init__(self):
        super().__init__(query=self.query)


class RentingLocations(QueryTable):
    query = """
    select
        objectId as locationId,
        name as locatieNaam
    from locations l2 
    where parentObjectId in (
        select objectId from locations l
        where name like '%Huur%'
        or name like '%prins willemhof%'
    ) or objectId in (
        select objectId from locations l
        where name like '%Huur%'
        or name like '%prins willemhof%'
    )
    """

    def __init__(self):
        super().__init__(query=self.query)


class LocationsForPsychogeriatric(QueryTable):
    # 8 = Bosch en Duin: 186, 236, 213, 237, 163, 239, 230, 238
    # 12 = Quintus: 48, 71, 111, 125
    # 75 = het Uiterjoon: 138
    # 489 = Zeewinde: 490, 507, 524, 533
    query = """
        select
            objectId as afdelingId,
            name as afdelingNaam
        from locations l 
        where objectId in (
            490, # ZW Boswinde
            507, # ZW Parkdael
            524, # ZW Duinwinde
            533, # ZW Parkzicht
            138, # HU Het Anker
            48,  # QS Aanlegsteiger
            71,  # QS Loggerthe
            111, # QS Nooder Den
            125, # QS Zuider Den
            186, # BD Duinrier
            236, # BD Zeeanemoon
            213, # BD Duinroos
            237, # BD Zeeappel
            163, # BD Duindoorn
            239, # BD Zeekraal
            230, # BD Duinviool
            238  # BD Zee-egel
        )
        """

    def __init__(self):
        super().__init__(query=self.query)


class Employees(QueryTable):
    query = """
        select
            e.objectId as employeeObjectId,
            e.emailAddress as employeeEmail
        from employees e
        where e.dateOfBirth is not Null
        """

    def __init__(self):
        super().__init__(query=self.query)
        isEmpty = self.dataframe['employeeEmail'] == ""
        self.dataframe = self.dataframe[~isEmpty]
