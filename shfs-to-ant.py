#!/usr/bin/env python
# coding: utf-8

import numpy as np 
import pandas as pd 
from simple_salesforce import Salesforce
from datetime import date  

## Initializations
source_org_url = '' 
source_org_session_id = ''
destination_org_url = ''
destination_org_session_id = ''

source_org = Salesforce(instance_url=source_org_url, session_id=source_org_session_id)
destination_org = Salesforce(instance_url=destination_org_url, session_id=destination_org_session_id)

household_id = '' # houehold to migrate from SHFS instance to ANT instance
record_origin = 'UWGN' 

## Begin Household/Account
#HH Account Lambda Functions
def name_lambda(row):
    new_name = row['Name'][:row['Name'].find("(")-1]
    new_name += ' Household'
    return new_name

def emergency_contact_lambda(row):
    if(str(row['BillingCity'])=='' or pd.isna(row['BillingCity'])):
        return ''
    else:
        return str(row['BillingStreet']) + ", " + str(row['BillingCity']) + ", " + str(row['BillingState']) + " " + str(int(row['BillingPostalCode']))

# Find Record Type on Dest Org
record_type = destination_org.query("SELECT Id FROM RecordType WHERE DeveloperName='HH_Account' AND SobjectType='Account'")
data = source_org.query("Select Id, Name, Website, Phone, Type, Email__c,  Emergency_Contact_Name__c, Email_Emergency_Contact__c, Phone_Emergency_Contact__c, BillingStreet, BillingCity, BillingState, BillingPostalCode, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, Living_Situation_Current__c  from Account WHERE Id='{}'".format(household_id))

# Transform Data Frame for Insert
df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)
df.insert(1,'Record_Origin__c',record_origin)
df = df.rename(columns={'Id':'External_Id__c', 'Email_Emergency_Contact__c': 'Emergency_Contact_Email__c', 'Living_Situation_Current__c': 'Client_Current_Living_Situation__c', 'Phone_Emergency_Contact__c': 'Emergency_Contact_Phone__c'})
df.insert(3,'RecordTypeId',record_type['records'][0]['Id'])
df['Name'] = df.apply(lambda row: name_lambda(row), axis=1)
df.insert(12,'Emergency_Contact_Address__c','')
df['Emergency_Contact_Address__c'] = df.apply(lambda row: emergency_contact_lambda(row), axis=1)

#insert records
records_to_insert = df.to_dict('records')
household = []

#Save household id (Don't actually need the loop now, but may in future if doing multiple HHs at once) 
for record in records_to_insert:
    result = destination_org.Account.create(record)
    household.append(result)
    
new_household_id = household[0]['id']

# Start of CONTACTS
# Contact lambda functions
def ethnicity_lambda(row):
    if(row['Ethnicity__c']=='Non-Hispanic/Non-Latino/Non-Latinx/Non-Latine'):
        return 'Non-Hispanic/Non-Latino'
    elif(row['Ethnicity__c']=='Hispanic/Latino/Latina/Latinx/Latine'):
        return 'Hispanic/Latino'
    elif(row['Ethnicity__c']=='Hispanic/Latino/Latinx/Latine'):
        return 'Hispanic/Latino'
    else:
        return row['Ethnicity__c']

def race_lambda(row):
    if(row['Race__c']=='Black or African American'):
        return 'Black/African American'
    else:
        return row['Race__c']

def secondary_race_lambda(row):
    if(row['Secondary_Race__c']=='Black or African American'):
        return 'Black/African American'
    else:
        return row['Secondary_Race__c']

# Get record type id from destination org
record_type = destination_org.query("SELECT Id FROM RecordType WHERE DeveloperName='Client' AND SobjectType='Contact'")
data = source_org.query("Select Id,Birthdate,Email,Ethnicity__c,FirstName,Gender__c,LastName,MailingCity,MailingPostalCode,MailingState,MailingStreet,Phone,Race__c,Relationship_to_Head_of_Household__c,Salutation,Secondary_Race__c,Veteran_Status__c FROM Contact WHERE AccountId='{}'".format(household_id))

df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)

account_id = destination_org.query("SELECT Id from Account WHERE External_Id__c='{}'".format(household_id))

df.insert(1,'Record_Origin__c',record_origin)
df.insert(2,'AccountId',new_household_id)
df = df.rename(columns={'Id':'External_Id__c'})
df.insert(3,'RecordTypeId',record_type['records'][0]['Id'])
df.insert(4,'Client_Doesn_t_Know__c',"true")
df.insert(5,'Current_member_of_household__c',"Yes")
df['Ethnicity__c'] = df.apply(lambda row: ethnicity_lambda(row), axis=1)
df['Race__c'] = df.apply(lambda row: race_lambda(row), axis=1)
df['Secondary_Race__c'] = df.apply(lambda row: secondary_race_lambda(row), axis=1)

# Insert contacts
new_contacts = []

records_to_insert = df.to_dict('records')

for record in records_to_insert:
    result = destination_org.Contact.create(record)
    new_contacts.append(result)
    
new_contact_ids = []

for i in new_contacts:
    new_contact_ids.append(i['id'])
                           
df['Id'] = new_contact_ids
new_contacts = df # Save contact ID in destination org to a new DF named new_contacts

# BEGIN PE TFC
# PE TFC Lambda Functions

def salesforce_roi_lambda(row):
    if(row['Salesforce_ROI_Uploaded__c']==0):
        return 'N/A'
    elif(row['Salesforce_ROI_Uploaded__c']==1):
        return 'Uploaded'

def verbal_consent_lambda(row):
    if(row['Verbal_Consent_Only__c']==0):
        return 'N/A'
    elif(row['Verbal_Consent_Only__c']==1):
        return 'Uploaded'

def mnps_roi_lambda(row):
    if(row['MNPS_ROI__c']==0):
        return 'N/A'
    elif(row['MNPS_ROI__c']==1):
        return 'Uploaded'

def hmis_roi_lambda(row):
    if(row['HMIS_ROI_Uploaded_del__c']==0):
        return 'N/A'
    elif(row['HMIS_ROI_Uploaded_del__c']==1):
        return 'Uploaded'

def name_lambda(row):
    month_loc = row['Open_Date__c'].find('/')
    month = row['Open_Date__c'][:month_loc]
    day_loc = month_loc + 1 + row['Open_Date__c'][month_loc+1:].find('/')
    day = row['Open_Date__c'][month_loc+1:day_loc]
    if (len(month) == 1):
        month = '0' + month
    if (len(day) == 1):
        day = '0' + day

    year = row['Open_Date__c'][day_loc+1:]
    # open_date = date(int(year), int(month), int(day)).isoformat()
    # new_name = row['Household/Organization Name'] + open_date
    return row['Name'] + ": " + str(year) + "-" + str(month) + "-" + str(day)

# Get data from source org, record type and program from destination org
record_type = destination_org.query("SELECT Id FROM RecordType WHERE DeveloperName='Family_Collective_Enrollment' AND SobjectType='pmdm__ProgramEngagement__c'")
program = destination_org.query("Select Id from pmdm__Program__c WHERE Name='The Family Collective'")
data = source_org.query("SELECT Household_Organization__r.Id, Household_Organization__r.Salesforce_ROI_Uploaded__c, Household_Organization__r.Verbal_Consent_Only__c, Household_Organization__r.MNPS_ROI__c, 	Household_Organization__r.HMIS_ROI_Uploaded_del__c, Household_Organization__r.Name, Open_Date__c, Living_Situation_at_Entry__c, Closure_Date__c FROM Enrollment_History__c WHERE Household_Organization__r.Id='{}'".format(household_id))

# reformatting dictionary with fields from multiple objects
household_fields = data['records'][0]['Household_Organization__r']
household_fields.pop('attributes')
household_df = pd.DataFrame(household_fields,index=[0])
household_df

enrollment_data = source_org.query("SELECT Open_Date__c, Living_Situation_at_Entry__c, Closure_Date__c FROM Enrollment_History__c WHERE Household_Organization__r.Id='{}'".format(household_id))
enrollment_data
enrollment_fields = enrollment_data['records'][0]
enrollment_fields.pop('attributes')
enrollment_df = pd.DataFrame(enrollment_fields,index=[0])

# combine household and enrollment fields
df = pd.concat([household_df, enrollment_df],axis=1, ignore_index=False) 

df.insert(1,'Record_Origin__c',record_origin)
df.insert(3,'RecordTypeId',record_type['records'][0]['Id'])
df.insert(4,'pmdm__Program__c',program['records'][0]['Id'])
df.insert(6,'pmdm__Account__c',new_household_id)
df.insert(8,'pmdm__Stage__c','Active')
df['Salesforce_ROI_Uploaded__c'] = df.apply(lambda row: salesforce_roi_lambda(row), axis=1)
df['MNPS_ROI__c'] = df.apply(lambda row: mnps_roi_lambda(row), axis=1)
df['HMIS_ROI_Uploaded_del__c'] = df.apply(lambda row: hmis_roi_lambda(row), axis=1)
df['Name'] = df.apply(lambda row: name_lambda(row), axis=1)

for index, row in new_contacts.iterrows():
    if (row['Relationship_to_Head_of_Household__c']=="Self"):
        df.insert(9,'pmdm__Contact__c',row['Id'])
        
df = df.rename(columns={"Living_Situation_at_Entry__c":"Living_Situation_Prior_to_Entry__c", "Open_Date__c":"pmdm__StartDate__c","Salesforce_ROI_Uploaded__c":"HMIS_Salesforce__c","Verbal_Consent_Only__c":"Verbal_Consent_Given_Salesforce_ROI__c","HMIS_ROI_Uploaded_del__c":"HMIS_ROI__c"})
df = df.drop('Id',axis=1)

new_pe_TFC = [] 
records_to_insert = df.to_dict('records')

for record in records_to_insert:
    result = destination_org.pmdm__programengagement__c.create(record)
    new_pe_TFC.append(result)
    
new_pe_TFC_ids = [] # only one should be created right now, so loop is unnecessary

for i in new_pe_TFC:
    new_pe_TFC_ids.append(i['id'])


# Start of PE Participant 
data = source_org.query("SELECT Id,AccountId, School_Name__c,School_Type__c,Current_childcare_child_education_status__c, enrolled_in_early_learning_program__c,Grade_Level_Child_at_Entry__c,Bankruptcy__c,FirstName,LastName,Birthdate,Relationship_to_head_of_household__c FROM Contact WHERE Account.Id='{}'".format(household_id))
acct_data = source_org.query("Select Name, Id, Open_Date_From_Most_Recent_Enrollment__c from Account WHERE Account.Id='{}'".format(household_id))
record_type = destination_org.query("SELECT Id FROM RecordType WHERE DeveloperName='Family_Collective_Participant_Enrollment' AND SobjectType='pmdm__ProgramEngagement__c'")

df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)
acct_df = pd.DataFrame(acct_data['records']).drop(['attributes'],axis=1) 

acct_df = acct_df.rename(columns={'Id':'AccountId'})
df = pd.merge(df, acct_df, on='AccountId')
df = df.rename(columns={'Id':'External_Id__c'})

df.insert(1,'Record_Origin__c',record_origin)
df.insert(3,'RecordTypeId',record_type['records'][0]['Id'])
df.insert(4,'pmdm__Program__c',program['records'][0]['Id'])
df.insert(6,'pmdm__Account__c',new_household_id)
df.insert(8,'pmdm__Stage__c','Active')
df.insert(9,'Family_Collective_Enrollment__c',new_pe_TFC_ids[0])
df.insert(10,'Client_Doesn_t_Know__c',True)

# insert account id and contact ids
new_contacts_ids = new_contacts[['External_Id__c','Id']]
new_contacts_ids = new_contacts_ids.rename(columns={'Id': 'pmdm__Contact__c'})

df = pd.merge(new_contacts_ids, df, on='External_Id__c')
del df['External_Id__c']
del df['AccountId']
df.head()
df = df.rename(columns={'Enrolled_in_Early_Learning_Program__c': 'Enrolled_in_TFC_Childcare__c', 
                        'Grade_Level_Child_at_Entry__c': 'Grade_Level_at_Entry__c',
                        'Open_Date_From_Most_Recent_Enrollment__c' : 'pmdm__StartDate__c',
                        'FirstName': 'First_Name__c',
                        'LastName': 'Last_Name__c',
                        'Birthdate': 'Birthdate__c'})
del df['Current_childcare_child_education_status__c']

new_pe_participants = [] 
records_to_insert = df.to_dict('records')

for record in records_to_insert:
    result = destination_org.pmdm__programengagement__c.create(record)
    new_pe_participants.append(result)
    
# Start of Case Notes
# Case note lambda functions
def add_creator_to_case_note(row):
    return row['Case_Note__c'] + " -" + row['CreatedByName']

def add_case_note_name(row):
    return row['Date__c'] + " Case Note"

data = source_org.query("SELECT Case_Note__c,Date__c,Household__c,Id,CreatedById FROM Case_Note__c WHERE Household__c='{}' AND Client_Interaction__c=null".format(household_id))

df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)

df.insert(1,'Record_Origin__c',record_origin)
del df['Household__c']
df.insert(2,'Household__c',new_pe_TFC_ids[0])

df = df.rename(columns={'Id':'External_Id__c'})

users = source_org.query("SELECT Id,Name from User")
users_df = pd.DataFrame(users['records']).drop(['attributes'],axis=1)
users_df = users_df.rename(columns={'Id': 'CreatedById', 'Name': 'CreatedByName'})
df = pd.merge(df, users_df, on='CreatedById')

df.insert(6,'New_Case_Note__c','')
df['Name'] = df.apply(lambda row: add_case_note_name(row), axis=1)
df['New_Case_Note__c'] = df.apply(lambda row: add_creator_to_case_note(row),axis=1)
del df['Case_Note__c']
del df['CreatedByName']
del df['CreatedById']
df = df.rename(columns={'New_Case_Note__c': 'Case_Note__c'})

records_to_insert = df.to_dict('records')

for record in records_to_insert:
    destination_org.case_note__c.create(record)

# Start of Sessions
# Sessions lambda functions
def add_session_note_name(row):
    return row['Session_Date__c'] + " Case Note"

def session_format_lambda(row):
    if(row['Session_Format__c']=='Video Conference/Zoom'):
        return 'Video Conference'
    else:
        return row['Session_Format__c']

#query for sessions related to household_id
data = source_org.query("SELECT Care_Team_Member__c,Case_Note_for_session__c,Household__c,Id,Name,RecordTypeId,Services_Rendered__c,Session_Date__c,Session_Format__c,Session_Length__c FROM Session__c WHERE Household__c='{}'".format(household_id))
df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)

#add record origin, replace household with PE TFC Id for destination org
df.insert(1,'Record_Origin__c',record_origin)
del df['Household__c']
df.insert(2,'pmdm__programengagement__c',new_pe_TFC_ids[0])

#get ids for care team member user records in source org
users = source_org.query("SELECT Id,Name from User")
users = pd.DataFrame(users['records']).drop(['attributes'],axis=1)

#get ids for care team member contacts in destination org
care_team_members = destination_org.query("SELECT Name,Id FROM Contact WHERE Record_Type_Name__c='Care Team Member'")
care_team_members = pd.DataFrame(care_team_members['records']).drop(['attributes'],axis=1)

users = users.rename(columns={'Id':'SourceOrgUserId'})
care_team_members = care_team_members.rename(columns={'Id':'DestOrgContactId'})

care_team_members = pd.merge(care_team_members, users, on='Name')

#get id for recordType in dest org
record_type = destination_org.query("SELECT Id FROM RecordType WHERE DeveloperName='Session' AND SobjectType='pmdm__ServiceDelivery__c'")
del df['RecordTypeId']

df.insert(1,'RecordTypeId',record_type['records'][0]['Id'])
df = df.rename(columns={'Id':'External_Id__c', 'Care_Team_Member__c':'SourceOrgUserId'})
df = pd.merge(df, care_team_members, on='SourceOrgUserId')
df = df.rename(columns={'DestOrgContactId': 'Primary_Care_Team_Member__c','Case_Note_for_session__c':'Session_Notes__c','Services_Rendered__c':'Services_Received_select_all_that_apply__c','Session_Length__c':'pmdm__quantity__c'})
del df['SourceOrgUserId']
del df['Name_x']
del df['Name_y']
df.insert(2,'Name','')
df['Name'] = df.apply(lambda row: add_session_note_name(row), axis=1)
df['Session_Format__c'] = df.apply(lambda row: session_format_lambda(row), axis=1)

records_to_insert = df.to_dict('records')

for record in records_to_insert:
    destination_org.pmdm__servicedelivery__c.create(record)

# begin client goals
data = source_org.query("SELECT Date_Goal_Achieved__c,Date_Goal_Set__c,Detailed_Goal_Description__c,Goal_Status__c,Household_Account__c,Id,Outcome__c,Pillar__c,Specific_Goal_Action_Step__c FROM Client_Goal__c WHERE Household_Account__c='{}'".format(household_id))

df = pd.DataFrame(data['records']).drop(['attributes'],axis=1)

df.insert(1,'Record_Origin__c',record_origin)
df = df.rename(columns={'Id':'External_Id__c','Detailed_Goal_Description__c':'Detailed_description_of_goal_optional__c', 'Pillar__c':'Domain__c'})
df.insert(2,'Household__c',new_pe_TFC_ids[0])
del df['Household_Account__c']
records_to_insert = df.to_dict('records')

for record in records_to_insert:
    destination_org.client_goal__c.create(record)

# start of Partner Agency Enrollment -- not finished
enrollment_id = source_org.query("Select Current_Enrollment_History__c from Account Where Id='{}'".format(household_id))
data = source_org.query("Select Id, Open_Date__c, Close_Date__c, Case_Manager_Enrollment__c, Employment_Navigator__c, Housing_Specialist__c, Agency__c from Enrollment_History__c Where Id='{}'".format(enrollment_id['records'][0]['Current_Enrollment_History__c']))
users = source_org.query("SELECT Id,Name from User")
users_df = pd.DataFrame(users['records']).drop(['attributes'],axis=1)

data = pd.DataFrame(data['records']).drop(['attributes'],axis=1)
dest_care_team_members = destination_org.query("Select Id, Name from Contact Where Record_Type_Name__c='Care Team Member'")
users_df = users_df.rename(columns={'Name': 'User_Name'})
dest_care_team_members = pd.DataFrame(dest_care_team_members['records']).drop(['attributes'],axis=1)

dest_agency_str = data['Agency__c'][0]
dest_agency_str = dest_agency_str.replace("'", r"\'")
dest_agency_str
dest_agency = destination_org.query("Select Id from Account WHERE Name='{}'".format(dest_agency_str))

users_df = users_df.rename(columns={'User_Name': 'Coach_User_Name', 'Id': 'Case_Manager_Enrollment__c'})
data = pd.merge(data, users_df, on='Case_Manager_Enrollment__c')
users_df = users_df.rename(columns={'Coach_User_Name': 'EN_User_Name', 'Case_Manager_Enrollment__c': 'Employment_Navigator__c'})
data = pd.merge(data, users_df, how='left', on='Employment_Navigator__c')
users_df = users_df.rename(columns={'EN_User_Name': 'HS_User_Name', 'Employment_Navigator__c': 'Housing_Specialist__c'})
data = pd.merge(data, users_df, how='left', on='Housing_Specialist__c')
dest_care_team_members = dest_care_team_members.rename(columns={'Name': 'Coach_User_Name', 'Id': 'Coach__c'})
data = pd.merge(data, dest_care_team_members, how='left', on='Coach_User_Name')
dest_care_team_members = dest_care_team_members.rename(columns={'Coach_User_Name': 'EN_User_Name', 'Coach__c': 'Employment_Navigator__c'})
del data['Employment_Navigator__c']
del data['Housing_Specialist__c']

data = pd.merge(data, dest_care_team_members, how='left', on='EN_User_Name')
dest_care_team_members = dest_care_team_members.rename(columns={'EN_User_Name': 'HS_User_Name', 'Employment_Navigator__c': 'Housing_Specialist__c'})
data = pd.merge(data, dest_care_team_members, how='left', on='HS_User_Name')
data.insert(2,'Household__c',new_pe_TFC_ids[0])

del data['Coach_User_Name']
del data['Case_Manager_Enrollment__c']
data = data.rename(columns={'Id': 'External_Id__c', 
                            'Open_Date__c': 'Start_Date__c',
                            'Close_Date__c': 'Exit_Date__c'})

del data['EN_User_Name']
del data['HS_User_Name']
del data['Agency__c']
data.insert(1,'Agency__c',dest_agency['records'][0]['Id'])

data = data.replace({np.nan: None}) # replace NaN with None (null)

records_to_insert = data.to_dict('records')

for record in records_to_insert:
    destination_org.partner_agency_enrollment__c.create(record)

## Remaining - Income, Wage Details, Education, Employment, Housing Preventions/Placements,  Financial Assistance, Assessments

