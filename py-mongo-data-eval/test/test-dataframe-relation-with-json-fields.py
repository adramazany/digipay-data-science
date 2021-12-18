import json
import yaml

import pandas as pd
import numpy as np

df1 = pd.DataFrame({'a': ['1', '2'],
                       'b': ['45.8', '73.9'],
                       'c': [10.5, 3.7],
                       'd': ['ali', 12],
                       'e': ['hasan', 34.44],
                       'e1': [None, 34.44],
                       'e2': ['ali',None],
                       'json_field': ['[{"name":"ali","family":"ram","age":"18","f11":"v11"},{"name":"mohammad","family":"ram","age":"28","f12":"v12"}]',
                                 '[{"name":"hassan","family":"ahmadi","age":"118","f21":"v21"},{"name":"reza","family":"ahmadi","age":"128","f22":"v22"}]'],
                       'f': ['test1', 'test2']
                   })

df = pd.DataFrame({
    "_id": {
        "0": "600a772ad36fa09fbaae4ae7",
        "1": "600a7affd36fa09fbaae4b0b"
    },
    "cellNumber": {
        "0": "09125186130",
        "1": "09104974350"
    },
    "userId": {
        "0": "dcdce32d-d24a-4732-b621-1bb63b0e19ed",
        "1": "6792abce-4a1c-4ad3-8cff-1a4b44b8f313"
    },
    "iban": {
        "0": "IR470130100000000035327856",
        "1": "IR850120020000008634227917"
    },
    "steps": {
        "0": "[{code: REGISTER, option: MANDATORY, processType: USER_PROCESS, startedDate: 1611298602374, completedDate: 1611298602374}, {code: DIGIPAY_SCORE, option: MANDATORY, processType: SYSTEM_PROCESS, stepResult: 51, startedDate: 1611298602384, completedDate: 1611298602384}, {code: PROFILE, option: MANDATORY, processType: USER_PROCESS, startedDate: 1611299251683, completedDate: 1611299251683}, {code: BANK_SCORE, option: MANDATORY, processType: USER_PROCESS, stepResult: 667, startedDate: 1611302116664, completedDate: 1611302116664}, {code: UPLOAD, option: MANDATORY, processType: USER_PROCESS, documents: [{title: \u062a\u0635\u0648\u06cc\u0631 \u0631\u0648\u06cc \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc, tag: NATIONAL_CARD_FRONT, option: MANDATORY, status: ACCEPTED, uploadDate: 1611302667696, opsActionDate: 1611412925252, docId: credit-activation\/722bb725-5854-486a-94b2-67260f7ca935.jpg}, {title: \u062a\u0635\u0648\u06cc\u0631 \u067e\u0634\u062a \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc, tag: NATIONAL_CARD_BACK, option: MANDATORY, status: ACCEPTED, uploadDate: 1611469265467, opsActionDate: 1619509884314, description: \u0644\u0637\u0641\u0627 \u062a\u0635\u0648\u06cc\u0631 \u0635\u0641\u062d\u0647 \u0627\u0648\u0644 \u0634\u0646\u0627\u0633\u0646\u0627\u0645\u0647 \u0631\u0627 \u062f\u0631 \u0627\u06cc\u0646 \u0642\u0633\u0645\u062a \u0627\u067e\u0644\u0648\u062f \u0646\u0645\u0627\u06cc\u06cc\u062f, docId: credit-activation\/0a12b02d-69ed-4506-b814-aa26079b8119.jpg}, {title: \u0645\u062f\u0627\u0631\u06a9 \u0645\u062d\u0644 \u0633\u06a9\u0648\u0646\u062a, tag: OWNERSHIP_DOCUMENT, option: OPTIONAL, status: INITIATED}, {title: \u0645\u062f\u0627\u0631\u06a9 \u0634\u063a\u0644\u06cc, tag: JOB_DOCUMENT, option: OPTIONAL, status: INITIATED}, {title: \u0633\u0627\u06cc\u0631 \u0645\u062f\u0627\u0631\u06a9, tag: OTHER_DOCUMENT, option: OPTIONAL, status: INITIATED}], startedDate: 1611302667696, completedDate: 1619509888594}, {code: CHEQUE_UPLOAD, option: MANDATORY, processType: USER_PROCESS, documents: [{title: \u062a\u0635\u0648\u06cc\u0631 \u0631\u0648\u06cc \u0686\u06a9, tag: CHEQUE_FRONT, option: MANDATORY, status: ACCEPTED, uploadDate: 1620283928140, opsActionDate: 1621852323967, docId: credit-activation\/d17724fe-9638-4e98-ac5c-344d5255f7a6.jpeg}, {title: \u062a\u0635\u0648\u06cc\u0631 \u067e\u0634\u062a \u0686\u06a9, tag: CHEQUE_BACK, option: MANDATORY_CONDITIONAL, status: ACCEPTED, uploadDate: 1621071090864, opsActionDate: 1621852330967, description: , docId: credit-activation\/54ae2327-c87e-4e76-acf9-8d487ae1d5b0.png}, {title: \u0645\u062f\u0631\u06a9 \u0647\u0648\u06cc\u062a\u06cc \u0635\u0627\u062d\u0628 \u0686\u06a9, tag: CHEQUE_IDENTIFICATION, option: MANDATORY_CONDITIONAL, status: ACCEPTED, uploadDate: 1620283976342, opsActionDate: 1621053538016, docId: credit-activation\/dfec9b0c-eb48-4bd3-835f-d6e2e8b33816.jpeg}], stepResult: RELATIVE, startedDate: 1620279096724, completedDate: 1622374661284}, {code: WALLET_ACTIVATION, option: MANDATORY, processType: USER_PROCESS, startedDate: 1622630409730, completedDate: 1623771344762}]",
        "1": "[{code: REGISTER, option: MANDATORY, processType: USER_PROCESS, startedDate: 1611299583490, completedDate: 1611299583490}, {code: DIGIPAY_SCORE, option: MANDATORY, processType: SYSTEM_PROCESS, stepResult: 51, startedDate: 1611299583500, completedDate: 1611299583500}, {code: PROFILE, option: MANDATORY, processType: USER_PROCESS, startedDate: 1611300163761, completedDate: 1611300163761}, {code: BANK_SCORE, option: MANDATORY, processType: USER_PROCESS, stepResult: 605, startedDate: 1611300416830, completedDate: 1611300416830}, {code: UPLOAD, option: MANDATORY, processType: USER_PROCESS, documents: [{title: \u062a\u0635\u0648\u06cc\u0631 \u0631\u0648\u06cc \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc, tag: NATIONAL_CARD_FRONT, option: MANDATORY, status: ACCEPTED, uploadDate: 1611301220815, opsActionDate: 1611412968489, docId: credit-activation\/f6370c2a-a90d-497f-ab7d-44b8114e8fb5.png}, {title: \u062a\u0635\u0648\u06cc\u0631 \u067e\u0634\u062a \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc, tag: NATIONAL_CARD_BACK, option: MANDATORY, status: ACCEPTED, uploadDate: 1611413474229, opsActionDate: 1619509904153, description: \u0644\u0637\u0641\u0627 \u0639\u06a9\u0633 \u0627\u0632 \u0627\u0635\u0644 \u067e\u0634\u062a \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc \u0642\u062f\u06cc\u0645\u06cc \u0628\u0647 \u0647\u0645\u0631\u0627\u0647 \u0631\u0633\u06cc\u062f (\u062f\u0631 \u06a9\u0646\u0627\u0631 \u0647\u0645) \u0631\u0627 \u0622\u067e\u0644\u0648\u062f \u0646\u0645\u0627\u06cc\u06cc\u062f. \u062f\u0631 \u0635\u0648\u0631\u062a \u0646\u0628\u0648\u062f \u0631\u0633\u06cc\u062f\u060c \u0635\u0641\u062d\u0647 \u0627\u0648\u0644 \u0634\u0646\u0627\u0633\u0646\u0627\u0645\u0647 \u062f\u0631 \u06a9\u0646\u0627\u0631 \u067e\u0634\u062a \u06a9\u0627\u0631\u062a \u0645\u0644\u06cc \u0642\u062f\u06cc\u0645\u06cc \u0627\u0631\u0633\u0627\u0644 \u06af\u0631\u062f\u062f.\\n, docId: credit-activation\/715c2b48-1411-4b02-941a-d48193d0886f.png}, {title: \u0645\u062f\u0627\u0631\u06a9 \u0645\u062d\u0644 \u0633\u06a9\u0648\u0646\u062a, tag: OWNERSHIP_DOCUMENT, option: OPTIONAL, status: INITIATED}, {title: \u0645\u062f\u0627\u0631\u06a9 \u0634\u063a\u0644\u06cc, tag: JOB_DOCUMENT, option: OPTIONAL, status: INITIATED}, {title: \u0633\u0627\u06cc\u0631 \u0645\u062f\u0627\u0631\u06a9, tag: OTHER_DOCUMENT, option: OPTIONAL, status: INITIATED}], startedDate: 1611301220815, completedDate: 1619509908557}, {code: CHEQUE_UPLOAD, option: MANDATORY, processType: USER_PROCESS, documents: [{title: \u062a\u0635\u0648\u06cc\u0631 \u0631\u0648\u06cc \u0686\u06a9, tag: CHEQUE_FRONT, option: MANDATORY, status: INITIATED}, {title: \u062a\u0635\u0648\u06cc\u0631 \u067e\u0634\u062a \u0686\u06a9, tag: CHEQUE_BACK, option: MANDATORY_CONDITIONAL, status: INITIATED}, {title: \u0645\u062f\u0631\u06a9 \u0647\u0648\u06cc\u062a\u06cc \u0635\u0627\u062d\u0628 \u0686\u06a9, tag: CHEQUE_IDENTIFICATION, option: MANDATORY_CONDITIONAL, status: INITIATED}]}, {code: WALLET_ACTIVATION, option: MANDATORY, processType: USER_PROCESS}]"
    },
    "status": {
        "0": "ACTIVE",
        "1": "UPLOAD_COMPLETED"
    },
    "creationDate": {
        "0": "1611298602374",
        "1": "1611299583490"
    },
    "statusModificationDate": {
        "0": "1623771344762",
        "1": "1619509908557"
    },
    "dueDate": {
        "0": "1624735799000",
        "1": "1624735799000"
    },
    "version": {
        "0": "32",
        "1": "15"
    },
    "installmentCount": {
        "0": "12",
        "1": "12"
    },
    "initialBalance": {
        "0": "100000000",
        "1": "150000000"
    },
    "_class": {
        "0": "com.digipay.creditonboarding.model.activation.Activation",
        "1": "com.digipay.creditonboarding.model.activation.Activation"
    },
    "creditId": {
        "0": "11570906731627978833918",
        "1": "12181188161627978833918"
    },
    "group._id": {
        "0": "103",
        "1": "101"
    },
    "group.title": {
        "0": "\u067e\u0644\u0646 1012 \u062c\u062f\u06cc\u062f",
        "1": "\u067e\u0644\u0646 1512 \u062c\u062f\u06cc\u062f"
    },
    "group.groupId": {
        "0": "70e84c47-38e3-420e-967a-4230657d4395",
        "1": "7190f718-0e0e-4582-8dfb-c968f0761408"
    },
    "group.ruleId": {
        "0": "PL-S-C-000204-10101212",
        "1": "PL-S-C-000203-15151212"
    },
    "group.fundProvider._id": {
        "0": "1",
        "1": "1"
    },
    "group.fundProvider.businessId": {
        "0": "96c150f1-1a85-46b4-825a-5fba9742c0cd",
        "1": "96c150f1-1a85-46b4-825a-5fba9742c0cd"
    },
    "group.fundProvider.activationTime": {
        "0": "2019-08-05 11:36:00",
        "1": "2019-08-05 11:36:00"
    },
    "group.fundProvider.modificationTime": {
        "0": "2019-08-05 11:36:00",
        "1": "2019-08-05 11:36:00"
    },
    "group.fundProvider.active": {
        "0": "True",
        "1": "True"
    },
    "group.fundProvider.name": {
        "0": "\u0644\u06cc\u0632\u06cc\u0646\u06af \u067e\u0627\u0631\u0633\u06cc\u0627\u0646",
        "1": "\u0644\u06cc\u0632\u06cc\u0646\u06af \u067e\u0627\u0631\u0633\u06cc\u0627\u0646"
    },
    "group.fundProvider.crn": {
        "0": "411573785193517119",
        "1": "411573785193517119"
    },
    "group.fundProvider.code": {
        "0": "PARSIAN",
        "1": "PARSIAN"
    },
    "group.fundProvider.type": {
        "0": "LEASING",
        "1": "LEASING"
    },
    "group.creationTime": {
        "0": "2021-05-03 10:43:15",
        "1": "2021-05-03 10:43:15"
    },
    "group.userEntryPoint": {
        "0": "0",
        "1": "0"
    },
    "group.active": {
        "0": "True",
        "1": "True"
    },
    "group.version": {
        "0": "0",
        "1": "0"
    }
})



print('df=',df)

all_json=list()
for a_steps in df['steps'].values.tolist():
    steps = yaml.load(a_steps,yaml.FullLoader)
    all_json = all_json+steps

print("all_json=",all_json)
# print("all_json.keys=",all_json.keys)

# df2 = pd.DataFrame.from_records([s.__dict__ for s in all_json])
# df2 = pd.DataFrame.from_records(all_json)
df2=pd.DataFrame(all_json)
print("df2=",df2)

