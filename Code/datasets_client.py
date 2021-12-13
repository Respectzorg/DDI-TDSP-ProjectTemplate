import configuration as config
import pandas as pd
from datetime import datetime
from configuration import conn_engine
from queryTable import QueryTable
import datasets as ds


class ClientObjectTable(QueryTable):
    """
    - Superclass of tables that have at least one column with clientObjectId.
    - This object includes functions to add client information to the dataframe.
    """
    query = "select objectId as clientObjectId from clients"

    def __init__(self, query=query):
        super().__init__(query)

    @property
    # total amount of unique clients
    def total_clients(self) -> int:
        return len(self.dataframe['clientObjectId'].unique())

    def lookupId(self, clientObjectId: int):
        return self.dataframe[
            self.dataframe['clientObjectId'] == clientObjectId]

    def addAllClientInfo(self):
        """ Adds the following client information to the objects dataframe: identification number, BSN, name, gender, email adres, mobile phone number."""
        self.addIdentificationNumber()
        self.addClientBSN()
        self.addClientName()
        self.addClientGender()
        self.addClientEmail()
        self.addClientMobile()

    def addIdentificationNumber(self):
        """ Adds clients identification number to the objects dataframe."""
        self.dataframe = self.addClientColumns("identificationNo")

    def addClientBSN(self):
        """ Adds clients bsn number to the objects dataframe."""
        self.dataframe = self.addClientColumns("bsn as clientBsn")

    def addClientName(self):
        """ Adds clients name to the objects dataframe."""
        self.dataframe = self.addClientColumns("name as clientName")

    def addClientGender(self):
        """ Adds clients gender to the objects dataframe."""
        self.dataframe = self.addClientColumns("gender as clientGender")

    def addClientEmail(self):
        """ Adds clients email to the objects dataframe."""
        self.dataframe = self.addClientColumns("emailAddress as clientEmail")

    def addClientMobile(self):
        """ Adds clients mobile phone number to the objects dataframe. """
        self.dataframe = self.addClientColumns("mobilePhone as clientMobile")

    def addClientColumns(self, columns: str):
        """ Adds columns of clients column from ONS to the objects dataframe"""
        df = self.dataframe.merge(
            pd.read_sql(
                f'select objectId, {columns} from clients',
                conn_engine
            ),
            left_on='clientObjectId',
            right_on='objectId',
            how='left'
        )
        return df[df.columns[~pd.Series(df.columns == 'objectId')]]

    def mergeOnClientId(self, other):
        """ Merges dataframe with other ClientObjectTables dataframe based on clientObjectId"""
        self.dataframe = self.dataframe.merge(
            other.dataframe,
            left_on='clientObjectId',
            right_on='clientObjectId',
            how='left'
        )


class ClientsInCare(ClientObjectTable):
    """
    Object based on dataframe which contains:

    | clientObjectId | createdAt | startCareDate | endCareDate |
    """
    query = f"""
        select
            ca.clientObjectId,
            ca.createdAt,
            ca.dateBegin as startCareDate,
            ca.dateEnd as endCareDate
         from care_allocations ca
        where dateEnd is Null
        or dateEnd > CURDATE()
        """

    def __init__(self):
        super().__init__(query=self.query)


class LocationAssignments(ClientObjectTable):
    # query main location of clients
    query = """
        select
            clientObjectId,
            locationObjectId,
            beginDate as startLocationDate,
            endDate as endLocationDate
        from location_assignments
        where (endDate is NULL or endDate > CURDATE())
        and locationType LIKE 'MAIN'
        """

    def __init__(self):
        super().__init__(query=self.query)


class LocationBasedClients(ClientObjectTable):
    def __init__(self, locationTableObject, idColumn='locationId', nameColumn='locatieNaam'):
        super().__init__()
        self.locatieNaam = nameColumn

        # Get datasets
        clients = ClientsInCare().dataframe
        assignments = LocationAssignments().dataframe
        location = locationTableObject.dataframe
        print(idColumn)
        # Combine clients, location and assignment datasets
        merged = assignments.merge(
            location,
            left_on='locationObjectId',
            right_on=idColumn,
            how='inner'
        )
        merged = merged.merge(
            clients,
            left_on='clientObjectId',
            right_on='clientObjectId',
            how='inner'
        )

        # Clean merged dataset
        cols = ['clientObjectId', 'startCareDate', 'endCareDate',
                'locationObjectId', self.locatieNaam, 'startLocationDate', 'endLocationDate']

        self.dataframe = merged[cols]
        self.dataframe = self.dataframe.drop_duplicates()

    @property
    def clientsPerLocation(self):
        return self.dataframe.groupby([self.locatieNaam]).nunique()['clientObjectId']


class IntramuralClients(LocationBasedClients):
    # clients that are assignt to an intramural location
    def __init__(self):
        super().__init__(ds.IntramuralLocations(),
                         idColumn='afdelingId', nameColumn='afdelingNaam')


class ExtramuralClients(LocationBasedClients):
    def __init__(self):
        super().__init__(ds.ExtramuralLocations())


class PgClients(LocationBasedClients):
    def __init__(self):
        super().__init__(ds.LocationsForPsychogeriatric(),
                         idColumn='afdelingId', nameColumn='afdelingNaam')


class Indications(ClientObjectTable):
    query = """
        select
            co.clientObjectId,
            ft.description as indication,
            co.beginDate as startIndicationDate,
            co.endDate as endIndicationDate
        from care_orders co
        join finance_types ft on ft.objectId = co.financeTypeObjectId
        where (co.endDate is NULL or co.endDate > CURDATE())
        """

    def __init__(self):
        super().__init__(query=self.query)

        # Data cleaning
        not_needed = ['Indicatievrije WLZ', 'Interne doorbelasting',
                      'Niet declarabel', 'Onderaannemerschap']

        for nn in not_needed:
            self.dataframe = self.dataframe[self.dataframe['indication'] != nn]

        # Data processing
        value_pairs = [
            ('AWBZ extramuraal', 'WLZ'),
            ('Wet maatschappelijke ondersteuning', 'WMO'),
            ('Zorgverzekeringswet', 'ZVW'),
            ('Paramedische Eerstelijnszorg', 'PEZ')
        ]

        for (from_value, to_value) in value_pairs:
            self.replaceValueContainedInColumn(
                from_value, to_value, 'indication')

        # dataframe with clientObejctId and indications in a list
        self.aggregated_indications = self.dataframe[[
            'clientObjectId', 'indication']].drop_duplicates()
        self.aggregated_indications = self.aggregated_indications.sort_values(by=['clientObjectId', 'indication']).groupby(
            ['clientObjectId']).agg(lambda x: ', '.join(x))['indication'].reset_index()


# TODO: (LOW prioriteit)
# Change to EEVers
class ResponsibleEmployees(ClientObjectTable):
    query = """
        select
            most_recent_relation.clientObjectId,
            cer_info.employeeObjectId,
            cer_info.relationName,
            cer_info.validFrom as startRelationDate,
            cer_info.validTo as endRelationDate
        from (
            select clientObjectId, max(createdAt) as most_recent
            from client_employee_relations cer
            where (cer.validTo is NULL or cer.validTo > CURDATE())
            group by clientObjectId
        ) most_recent_relation
        inner join client_employee_relations cer_info on
            cer_info.clientObjectId = most_recent_relation.clientObjectId
            and cer_info.createdAt = most_recent_relation.most_recent
        """

    def __init__(self):
        super().__init__(query=query)

        employees = ds.Employees().dataframe
        self.dataframe = self.dataframe.merge(
            employees,
            left_on='employeeObjectId',
            right_on='employeeObjectId',
            how='left'
        )


class LegalRepresentative(ClientObjectTable):
    query = """
    select
        clientObjectId,
        nccrt.name relatieType,
        nrtc.name relatieCategorie,
        r.firstName fistNameWV,
        r.birthNamePrefix prefixWV,
        r.birthName birthnameWV,
        r.updatedAt,
        a.telephoneNumber,
        a.telephoneNumber2,
        a.email
    from relations r
    left join nexus_client_contact_relation_types nccrt on nccrt.objectId  = r.clientContactRelationTypeId
    left join nexus_relation_type_categories nrtc on nrtc.objectId = nccrt.categoryObjectId
    left join relations_addresses ra on ra.relationObjectId = r.objectId
    left join addresses a on ra.addressObjectId = a.objectId
    where nccrt.name like '%wettelijke%'
    """

    def __init__(self):
        super().__init__(query=self.query)


class ActiveCareplans(ClientObjectTable):
    # TODO: make correct selection based on indicators
#     query = """
#     select
#         c.clientObjectId,
#         c.objectId as careplanId,
#         c.employeeObjectId as careplanEmployee,
#         c.beginDate as startCareplanDate,
#         c.endDate as endCareplanDate,
#         dcpsm.discussionType,
#         dcpsm.signatureRequired as signatureRequiredActive,
#         dcps.objectId as signedActive
#     from careplans c
#     left join dossier_care_plan_signatures dcps on dcps.carePlanObjectId = c.objectId
#     left join dossier_care_plan_signature_metadata dcpsm on dcpsm.carePlanObjectId = c.objectId
#     where c.status = 1 # 0 is Concept, 1 is Actief, 2 is Oud
#     and c.endDate >= CURDATE()
#     """

    def __init__(self):
        super().__init__(query=self.query)
        self._prepareDiscussionType()

    def _prepareDiscussionType(self):
        _DISCUSSIONTYPE = 'discussionType'
        isBesprokenEnAkkoord = self.dataframe[_DISCUSSIONTYPE] == 0
        self.dataframe.loc[
            isBesprokenEnAkkoord,
            _DISCUSSIONTYPE] = 'Is besproken en akkoord'
        wordtLaterBesproken = self.dataframe[_DISCUSSIONTYPE] == 1
        self.dataframe.loc[
            wordtLaterBesproken,
            _DISCUSSIONTYPE] = 'wordt later besproken'
        wordtNietBesproken = self.dataframe[_DISCUSSIONTYPE] == 2
        self.dataframe.loc[
            wordtNietBesproken,
            _DISCUSSIONTYPE] = 'wordt niet besproken'
        nietAkkoord = self.dataframe[_DISCUSSIONTYPE] == 3
        self.dataframe.loc[nietAkkoord, _DISCUSSIONTYPE] = 'niet akkoord'


class WithFirstCareplan(ClientObjectTable):
#     query = """
#     select
#         cp_info.clientObjectId,
#         dcpsm.signatureRequired as signatureRequiredFirst,
#         dcps.objectId as signedFirst,
#         cp_info.status as statusFirstPlan
#     from ( # first created careplan which isn't a concept
#         select c.objectId, min(c.createdAt) oldest_date, c.status
#         from careplans c
#         where c.status != 0
#         group by c.clientObjectId
#     ) oldest_cp
#     join careplans cp_info on
#         oldest_cp.objectId = cp_info.objectId
#         and oldest_cp.oldest_date = cp_info.createdAt
#     left join dossier_care_plan_signatures dcps on dcps.carePlanObjectId = oldest_cp.objectId
#     left join dossier_care_plan_signature_metadata dcpsm on dcpsm.carePlanObjectId = oldest_cp.objectId
#     """
    query = """
    select
        doc_info.clientObjectId,
        doc_info.employeeObjectId,
        doc_info.filename,
        doc_info.status,
        doc_info.updatedAt
    from (
        select d.objectId, max(d.updatedAt) as most_recent
        from document_tags dt
        join documents d on d.objectId = dt.documentObjectId
        # tag 14 = zorgovereenkomst
        where dt.tagObjectId = 32
        group by clientObjectId
    ) as mr
    inner join documents doc_info on
        doc_info.objectId = mr.objectId
        and doc_info.updatedAt = mr.most_recent
    """

    def __init__(self):
        super().__init__(query=self.query)
        self.dataframe = self.dataframe.merge(
            ActiveCareplans().dataframe,
            left_on='clientObjectId',
            right_on='clientObjectId',
            how='left'
        )


class Careagreements(ClientObjectTable):
    query = """
    select
        doc_info.clientObjectId,
        doc_info.employeeObjectId,
        doc_info.filename,
        doc_info.status,
        doc_info.updatedAt
    from (
        select d.objectId, max(d.updatedAt) as most_recent
        from document_tags dt
        join documents d on d.objectId = dt.documentObjectId
        # tag 14 = zorgovereenkomst
        where dt.tagObjectId = 14
        group by clientObjectId
    ) as mr
    inner join documents doc_info on
        doc_info.objectId = mr.objectId
        and doc_info.updatedAt = mr.most_recent
    """

    def __init__(self):
        super().__init__(query=self.query)
        self.dataframe['Getekend'] = self.dataframe['filename'].str.contains(
            'getekend')


class Notifications(ClientObjectTable):
    query = """
    select
        mr.clientObjectId,
        n_info.createdAt,
        n_info.title,
        n_info.subtitle
    from (
        select clientObjectId, max(createdAt) as most_recent
        from notifications n
        # client in zorg opgenomen = 152
        where n.notificationTypeObjectId = 152
        group by clientObjectId
    ) mr
    inner join notifications n_info on
        mr.clientObjectId = n_info.clientObjectId
        and mr.most_recent = n_info.createdAt
    """

    def __init__(self):
        super().__init__(query=self.query)


if __name__ == "__main__":
    intramuraalClients = IntramuraalClients()
    print(intramuraalClients.dataframe)


class Rekenmodule(ClientObjectTable):
    query = """
    select
        d.clientObjectId,
        d.updatedAt # meest recent
    from documents d
    where (
        filename like '%rekenmodule%'
        or description like '%rekenmodule%'
    ) and status != 3 #verwijderd
    group by d.clientObjectId
    """

    def __init__(self):
        super().__init__(query=self.query)
        self.dataframe['Rekenmodule'] = True
