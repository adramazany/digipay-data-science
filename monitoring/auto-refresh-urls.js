/* javascript in Firefox Tab Reloader (page auto refresh) addons
    https://addons.mozilla.org/en-US/firefox/addon/tab-reloader
*/
function random_reload(){
    URLS=[
           {name:'Health-حال این امروز'
             ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2Fmonitoring%2F_portal%2FHealth&Page=%D8%AD%D8%A7%D9%84%20%D8%A7%D9%85%D8%B1%D9%88%D8%B2&PageIdentifier=hkv8d2t804323kog&BookmarkState=4fs2mgkmckggim1vgvpf3065km&options=-'
         },{name:'کاربر جدید اپ و اکسپرس OKR'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=%20%D8%A8%D8%A7%20%D9%81%D8%B1%D8%B5%D8%AA%20%DB%8C%DA%A9%20%D9%85%D8%A7%D9%87%D9%87%20-%20%DA%A9%D8%A7%D8%B1%D8%A8%D8%B1%20%D8%AC%D8%AF%DB%8C%D8%AF%20%D8%A7%D9%BE%20%D9%88%20%D8%A7%DA%A9%D8%B3%D9%BE%D8%B1%D8%B3&PageIdentifier=3nocs7e35v1hmqk2&BookmarkState=pmbb63tfls00dtr4k2noogvbkq&options=rd'
        },{name:'Health-حال این هفته'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2Fmonitoring%2F_portal%2FHealth&Page=%D8%AD%D8%A7%D9%84%20%D8%A7%DB%8C%D9%86%20%D9%87%D9%81%D8%AA%D9%87&PageIdentifier=760g0807j32tib8s&BookmarkState=32493619n4e2n41acoqm2kcupi&options=-'
        },{name:'OKR تبدیل 1 م کاربر دیجی کالا'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=%D8%AA%D8%A8%D8%AF%DB%8C%D9%84%201%20%D9%85%20%DA%A9%D8%A7%D8%B1%D8%A8%D8%B1%20%D8%AF%DB%8C%D8%AC%DB%8C%20%DA%A9%D8%A7%D9%84%D8%A7&PageIdentifier=0dun5sblr4n7tq1p&BookmarkState=ph59j00bt81mc07j2kdj6f2r0m&options=rd'
        },{name:'OKR Wallet Based TXN Customers'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=Wallet%20Based%20TXN%20Customers&PageIdentifier=d8m89m0qbfk04lg0&BookmarkState=ojmjke4c211v27la23hm0n7j8u&options=rd'
        },{name:'OKR Credit Customer Service'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=Credit%20Customer%20Service&PageIdentifier=abbibnisuucq496b&BookmarkState=uutaro4jjcnpn29v77aedfqqge&options=rd'
        },{name:'OKR Credit OBNPL'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=Credit%20OBNPL&PageIdentifier=dmrq54t3dphiu55i&BookmarkState=md6uqsojm1rn1v3gsi14a4ucee&options=rd'
        },{name:'OKR Credit CBNPL'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=Credit%20CBNPL&PageIdentifier=5jpno8u061q5igbn&BookmarkState=6t2v7pecv664psgqkir78ig58q&options=rd'
        },{name:'OKR APP Retention Monthly'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FApp%2F_portal%2FOKR---APP&Page=Retention%20Rate&PageIdentifier=v3hbb9r6730t51k2&BookmarkState=cf4fkjre9rphi9krb7cnslct1u&options=rd'
        },{name:'Credit Funnel Summary-Total Monthly'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FCredit%2F_portal%2FCredit%20Funnel%20Summary&Page=Total%20Monthly&PageIdentifier=qfh9k0kkiir4g57g&BookmarkState=kvnf60iumpaq5btvvsft6ehof6&options=prd'
        },{name:'Credit User Journey-Duration of Activation - Total'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FCredit%2F_portal%2FUser%20Journey&Page=Duration%20of%20Activation%20-%20Total&PageIdentifier=ajgi4s9l3e9co5en&BookmarkState=i09fl9qjfr623ofe3nnut00vni&options=rd'
        },{name:'APP Transaction-Monthly Count & Amount of TNX'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FMarketing%2F_portal%2FAPP%20Transaction&Page=Monthly%20Count%20%26%20Amount%20of%20TNX&PageIdentifier=vr61it5fdmdm4bmk&BookmarkState=4gfcn4vh4dc1snb95tr1q7mgdi&options=rd'
        },{name:'Merchant Credit-Pre User'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FMerchant%20%20Credit%2F_portal%2FMerchant%20Credit&Page=Pre%20User&PageIdentifier=dnbll63hert0h07h&BookmarkState=hlad62je0hraj8sbboq19kkeam&options=-'
        },{name:'Operation-Credit Registration Performance daily'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FOperation%2F_portal%2FOperation%20Credit%20Reports&Page=Credit%20Registration%20Performance%20daily&PageIdentifier=09ubis4peba44g7o&BookmarkState=87q0pb7mvt9seulueldbms8tqi&options=rd'
        },{name:'Payment Reports-Wallet Customers & Users Monthly'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FPayment%2F_portal%2FPayment%20Reports&Page=Wallet%20Customers%20%26%20Users%20Monthly&PageIdentifier=tkhhqpihordl00s0&BookmarkState=7vsf98er3n9npmus5rp36sbd42&options=rd'
        },{name:'PR-final'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2FPR%2F_portal%2FPR&Page=final&PageIdentifier=g91v1lf3441ubovu&BookmarkState=9ejire0kbq73q3g43ka6ptq7gq&options=-'
        },{name:'health-حال این ماه'
            ,url:'http://newbi.mydigipay.com/analytics/saw.dll?Dashboard&PortalPath=%2Fshared%2Fmonitoring%2F_portal%2FHealth&Page=%D8%AD%D8%A7%D9%84%20%D8%A7%DB%8C%D9%86%20%D9%85%D8%A7%D9%87&PageIdentifier=o21uhmmgeako9c3v&BookmarkState=bss8df6mlvbqjsklnlk3f2jvqe&options=-'
        }
    ];
    rand_url = URLS[ Math.floor(Math.random() * URLS.length) ];
    console.log(rand_url);
    document.location.href =rand_url.url;
}
setTimeout(random_reload,60000);
