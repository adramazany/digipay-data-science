import yaml

st='[{"code": "REGISTER", "option": "MANDATORY", "processType": "USER_PROCESS", "startedDate": 1593956284214, "completedDate": 1593956284214}, {"code": "DIGIPAY_SCORE", "option": "MANDATORY", "processType": "USER_PROCESS", "stepResult": "51", "startedDate": 1593956284224, "completedDate": 1593956284224}, {"code": "PROFILE", "option": "MANDATORY", "processType": "USER_PROCESS", "startedDate": 1594230521460, "completedDate": 1594230521460}, {"code": "BANK_SCORE", "option": "MANDATORY", "processType": "USER_PROCESS", "stepResult": "-", "startedDate": 1594230921307, "completedDate": 1594230921307}, {"code": "UPLOAD", "option": "MANDATORY", "processType": "USER_PROCESS", "documents": [{"title": "تصویر روی کارت ملی", "tag": "NATIONAL_CARD_FRONT", "option": "MANDATORY", "status": "ACCEPTED", "uploadDate": 1594231107346, "opsActionDate": 1595156407194, "docId": "credit-activation/f10a8f3d-7d28-48df-854a-a76b6d1ff3c6.png"}, {"title": "تصویر پشت کارت ملی", "tag": "NATIONAL_CARD_BACK", "option": "MANDATORY", "status": "ACCEPTED", "uploadDate": 1595852033206, "opsActionDate": 1595916098505, "description": "تصویر اصل و رنگی پشت کارت ملی جدیدارسال نمایید. درصورت نبود: عکس از اصل پشت کارت ملی قدیمی به همراه رسیده (در کنار هم) را آپلود نمایید.در صورت نبود  رسید صفحه اول شناسنامه  در کنار کارت ملی قدیمی ارسال گردد\n", "docId": "credit-activation/0204ba73-03c3-4d3c-a67f-00efca3cff73.png"}, {"title": "مدارک محل سکونت", "tag": "OWNERSHIP_DOCUMENT", "option": "OPTIONAL", "status": "ACCEPTED", "uploadDate": 1594231318003, "opsActionDate": 1595156394181, "docId": "credit-activation/22f05e60-ee21-4071-8e11-26b4e2084006.png"}, {"title": "مدارک شغلی", "tag": "JOB_DOCUMENT", "option": "OPTIONAL", "status": "ACCEPTED", "uploadDate": 1595852180733, "opsActionDate": 1595916139203, "description": "لطفا صفحه اول و آخر گردش حساب 6 ماهه اخیر  را آپلود کنید که در آن،سربرگ بانکی،تاریخ،نام خود و مانده مشخص باشد.(مانده حساب در اخرین گردش با لای 2میلیون باشد)\n", "docId": "credit-activation/fbbebd70-f072-4f8b-8c59-07c938569651.png"}, {"title": "سایر مدارک", "tag": "OTHER_DOCUMENT", "option": "OPTIONAL", "status": "ACCEPTED", "uploadDate": 1595853897744, "opsActionDate": 1595916558782, "description": "، لطفا چک صیادی را به مبلغ صد و سی و پنج میلیون ریال در وجه لیزینگ پارسیان به تاریخ   (9 ماه بعد)پر نمایید.\n", "docId": "credit-activation/b88687b6-647c-4426-80ba-1d54a2830c7f.png"}], "startedDate": 1594231107346, "completedDate": 1595916578014}, {"code": "CHEQUE_UPLOAD", "option": "MANDATORY", "processType": "USER_PROCESS", "documents": [{"title": "تصویر روی چک", "tag": "CHEQUE_FRONT", "option": "MANDATORY", "status": "ACCEPTED", "uploadDate": 1595918899932, "opsActionDate": 1596010233178, "docId": "credit-activation/75a837a0-09ae-4af3-ab9f-4466e48937fd.png"}, {"title": "تصویر پشت چک", "tag": "CHEQUE_BACK", "option": "MANDATORY_CONDITIONAL", "status": "INITIATED"}, {"title": "مدرک هویتی صاحب چک", "tag": "CHEQUE_IDENTIFICATION", "option": "MANDATORY_CONDITIONAL", "status": "INITIATED"}], "stepResult": "INDIVIDUAL", "startedDate": 1595918849998, "completedDate": 1596461831837}, {"code": "WALLET_ACTIVATION", "option": "MANDATORY", "processType": "USER_PROCESS", "startedDate": 1596461831867, "completedDate": 1596465338604}]'
obj =yaml.load(st,yaml.loader.UnsafeLoader)
print(obj)