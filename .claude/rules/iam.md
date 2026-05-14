---
paths:
  - "cheeto/iam.py"
  - "cheeto/iamapi/**/*.py"
---

# Overview

Standards for the UC Davis IAM integration.

`cheeto/iam.py` is the hand-written client wrapper; `cheeto/iamapi/` is the
generated httpx-based API client and should not be hand-edited.


# API Results

`search_pri_kerb_acct`: given `user_id` (equivalent to `User.name`), returns JSON of shape:

```
[{'iamId': '1000000001',
  'userId': 'jdoe',
  'uuId': '100001',
  'createDate': '2020-01-01 00:00:00',
  'claimDate': None,
  'expireDate': None}]
```

Used when we don't yet have the user's IAM ID but do have their user name.

`search_contact_info`: given `email`, returns JSON of shape:

```
[{'iamId': '1000000001',
  'email': 'jdoe@example.edu',
  'hsEmail': None,
  'campusEmail': None,
  'addrStreet': None,
  'addrCity': None,
  'addrState': None,
  'addrZip': None,
  'postalAddress': None,
  'workPhone': '555-555-5555',
  'workCell': None,
  'workPager': None,
  'workFax': None}]
```

Used when we have email but no IAM ID or user name.

`get_person_using_iam_id`: given an IAM ID `iam_id`, returns JSON of shape:

```
[{'iamId': '1000000001',
  'mothraId': '01000001',
  'ppsId': None,
  'employeeId': '10000001',
  'studentId': None,
  'bannerPIdM': None,
  'externalId': None,
  'oFirstName': 'Jane',
  'oMiddleName': None,
  'oLastName': 'Doe',
  'oFullName': 'Jane Doe',
  'oSuffix': None,
  'dFirstName': 'Jane',
  'dMiddleName': None,
  'dLastName': 'Doe',
  'dSuffix': None,
  'dFullName': 'Jane Doe',
  'dPronouns': 'They/Them/Theirs',
  'isEmployee': True,
  'isHSEmployee': False,
  'isFaculty': False,
  'isStudent': False,
  'isStaff': True,
  'isExternal': False,
  'privacyCode': None,
  'modifyDate': '2026-01-01 00:00:00',
  'isCampusEmployee': 'Y',
  'userId': 'jdoe',
  'campusEmail': 'jdoe@example.edu'}]
```

Core user information.

`get_pps_assocs_using_iam_id`: given `iam_id`, returns departmental associations / titles. JSON shape:

```
[{'iamId': '1000000001',
  'deptCode': '999001',
  'deptOfficialName': 'EXAMPLE DEPARTMENT',
  'deptDisplayName': 'EXAMPLE DEPARTMENT',
  'deptAbbrev': 'EX DEPT',
  'isUCDHS': False,
  'bouOrgOId': '00000000000000000000000000000001',
  'adminDeptCode': '999001',
  'adminDeptOfficialName': 'EXAMPLE DEPARTMENT',
  'adminDeptDisplayName': 'EXAMPLE DEPARTMENT',
  'adminDeptAbbrev': 'EX DEPT',
  'adminIsUCDHS': False,
  'adminBouOrgOId': '00000000000000000000000000000001',
  'apptDeptCode': '999001',
  'apptDeptOfficialName': 'EXAMPLE DEPARTMENT',
  'apptDeptDisplayName': 'EXAMPLE DEPARTMENT',
  'apptDeptAbbrev': 'EX DEPT',
  'apptIsUCDHS': False,
  'apptBouOrgOId': '00000000000000000000000000000001',
  'assocRank': '1',
  'assocStartDate': '2020-01-01 00:00:00',
  'assocEndDate': None,
  'titleCode': '099999',
  'titleOfficialName': 'EXAMPLE-TITLE',
  'titleDisplayName': 'EXAMPLE-TITLE',
  'positionTypeCode': '5',
  'positionType': 'Academic',
  'percentFullTime': '1',
  'createDate': '2020-01-01 00:00:00',
  'modifyDate': '2026-01-01 00:00:00',
  'emplClass': '9',
  'emplClassDesc': 'Academic: Faculty',
  'emplPositionNumber': '40000001',
  'reportsToEmplID': '10000002',
  'reportsToPositionNum': '40000002',
  'reportsToIAMID': '1000000002'}]
```

`search_ppsbo_us`: Given `org_o_id`, the organizational business unit ID, returns JSON of shape:

```
[{'orgOId': '00000000000000000000000000000001',
  'deptCode': '99',
  'deptOfficialName': 'EXAMPLE COLLEGE',
  'deptDisplayName': 'EXAMPLE COLLEGE',
  'deptAbbrev': 'EXAMPLE COLLEGE',
  'isUCDHS': False,
  'createDate': '2010-01-01 00:00:00',
  'modifyDate': '2026-01-01 00:00:00'}]
```

This is information about the business unit a department belongs to; this basically matches up with what are called "colleges." 
