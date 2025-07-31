#Bu uygulamada ETL aracının performans ve fiyatlaması daha etkin bir şekilde analiz edebilmek ve takip edebilmek için ETL aracnın her bir süreç için 
#yaratmış olduğu log dosyalarını okuyup json formatından structure (database table) formatına dönüştürüp ilgili tabloya bu formattaki verinin phyton 
#ile yazılması amaçlanmıştır.
#ESRA AYVAZ

import json #json formatında çalışıyorum.
import pandas as pd #dataframe oluşturup excele yazdığım için gerekli.
import os #klasöre ulaşmam lazım.
import glob #klasördeki verileri çekmem lazım.

#verileri almak için istediğim dosyalar oluşturduğum JSON klasöründe
klasor_yolu = r"D:/JSON"

#verileri çekmek için dosyalara erişim
dosya_yollari = glob.glob(os.path.join(klasor_yolu, "*.json"))

tum_veriler = []

for dosya_yolu in dosya_yollari:
    with open(dosya_yolu, "r", encoding="utf-8-sig") as dosya:
        veri = json.load(dosya)
        
    #her bir activity için gerekli verileri çekiyorum.
    activities = veri.get("value", [])

    veri_listesi = []
    
    #eğer activity type SetVariable ise verileri tabloya yazdırmayacağım çünkü çoğu değerler null
    for activity in activities:
        if activity.get("activityType") == "SetVariable":
            continue

        kayit = {}

        #activity ve pipeline verileri
        kayit["activityName"] = activity.get('activityName')
        kayit["activityRunEnd"] = activity.get('activityRunEnd')
        kayit["activityRunId"] = activity.get('activityRunId')
        kayit["activityRunStart"] = activity.get('activityRunStart')
        kayit["activityType"] = activity.get('activityType')
        kayit["durationInMs"] = activity.get('durationInMs')
        kayit["pipelineName"] = activity.get('pipelineName')
        kayit["pipelineRunId"] = activity.get('pipelineRunId')
        kayit["status"] = activity.get('status')
        #kayit["recoveryStatus"] = activity.get('recoveryStatus') hepsi none
        #kayit["retryAttempt"] = activity.get('retryAttempt') hepsi null
        
        #billingReference verileri
        billing = activity.get("output", {}).get("billingReference", {})
        
        #liste şeklinde olduğu için parçaladım.
        bd = billing.get("billableDuration", [])
        if bd:
            bd0 = bd[0]
            kayit["billableDuration"] = bd0.get("duration")
            kayit["billable_meterType"] = bd0.get("meterType")
            kayit["billable_unit"] = bd0.get("unit")
        else:
            kayit["billableDuration"] = None
            kayit["billable_meterType"] = None
            kayit["billable_unit"] = None

        tb = billing.get("totalBillableDuration", [])
        if tb:
            tb0 = tb[0]
            kayit["totalBillableDuration"] = tb0.get("duration")
            kayit["totalBillable_meterType"] = tb0.get("meterType")
            kayit["totalBillable_unit"] = tb0.get("unit")
        else:
            kayit["totalBillableDuration"] = None
            kayit["totalBillable_meterType"] = None
            kayit["totalBillable_unit"] = None

        #output verilerinden gerekli olabilecekler
        output = activity.get("output", {})
        kayit["copyDuration"] = output.get("copyDuration")
        kayit["dataConsistencyVerification"] = output.get("dataConsistencyVerification", {}).get("VerificationResult") if output.get("dataConsistencyVerification") else None
        kayit["dataRead"] = output.get("dataRead")
        kayit["dataWritten"] = output.get("dataWritten")
        kayit["durationInQueue"] = output.get("durationInQueue", {}).get("integrationRuntimeQueue") if output.get("durationInQueue") else None
        kayit["effectiveIntegrationRuntime"] = output.get("effectiveIntegrationRuntime")

        #output içindeki executionDetails listesi eğer doluysa verileri yazdır boşsa none
        output_execution_details = output.get("executionDetails", [])

        if isinstance(output_execution_details, list) and len(output_execution_details) > 0:
            detail = output_execution_details[0]
            durations = detail.get("detailedDurations", {})
            profile_queue = detail.get("profile", {}).get("queue", {})

            kayit["queuingDuration"] = durations.get("queuingDuration")
            kayit["timeToFirstByte"] = durations.get("timeToFirstByte")
            kayit["transferDuration"] = durations.get("transferDuration")
            kayit["totalDuration"] = detail.get("duration")
            kayit["interimDataWritten"] = detail.get("interimDataWritten")
            kayit["interimRowsCopied"] = detail.get("interimRowsCopied")
            kayit["queueStatus"] = profile_queue.get("status")
            kayit["queueDuration"] = profile_queue.get("duration")
        else:
            for key in ["queuingDuration", "timeToFirstByte", "transferDuration", "totalDuration",
                        "interimDataWritten", "interimRowsCopied", "queueStatus", "queueDuration"]:
                kayit[key] = None

        #activity içindeki en dış executionDetails kısmındaki integrationRuntime verileri
        ext_execution_details = activity.get("executionDetails", {})

        if isinstance(ext_execution_details, dict):
            runtimes = ext_execution_details.get("integrationRuntime", [])
            if runtimes and isinstance(runtimes, list):
                runt0 = runtimes[0]
                kayit["integrationRuntime_name"] = runt0.get("name")
                kayit["integrationRuntime_type"] = runt0.get("type")
                kayit["integrationRuntime_location"] = runt0.get("location")
            else:
                kayit["integrationRuntime_name"] = None
                kayit["integrationRuntime_type"] = None
                kayit["integrationRuntime_location"] = None
                
        #eğer executionDetails dict değilse alanlar None        
        else:
            kayit["integrationRuntime_name"] = None
            kayit["integrationRuntime_type"] = None
            kayit["integrationRuntime_location"] = None

        #activity içindeki integrationRuntimeNames verisini al.
        kayit["integrationRuntimeName"] = None
        if "integrationRuntimeNames" in activity:
            if isinstance(activity["integrationRuntimeNames"], list) and len(activity["integrationRuntimeNames"]) > 0:
                kayit["integrationRuntimeName"] = activity["integrationRuntimeNames"][0]

        #Row verilerinden gerekli olabilecekler
        kayit["rowsCopied"] = output.get("rowsCopied")
        kayit["rowsRead"] = output.get("rowsRead")
        kayit["sinkPeakConnections"] = output.get("sinkPeakConnections")
        kayit["sourcePeakConnections"] = output.get("sourcePeakConnections")
        kayit["throughput"] = output.get("throughput")
        #kayit["sqlDwPolyBase"] = output.get("sqlDwPolyBase") çoğunda YANLIŞ yazıyordu gereksiz olabilir.
        #kayit["usedParallelCopies"] = output.get("usedParallelCopies") çoğunda 1 yazıyor gereksiz olabilir.

        veri_listesi.append(kayit) #tüm kayıtları topladım.

    tum_veriler.extend(veri_listesi) #topladığım kayıtları tüm veriler olarak listeledim.
    
#dataframe'e dönüştürüp excele yazdırdım.
df = pd.DataFrame(tum_veriler)
df.to_excel(r"D:\DATASET.xlsx", index=False)

#kod başarıyla çalışırsa bu mesaj yazdırılacak.
print("Tüm JSON dosyalarından veriler tek Excel dosyasında toplandı.")

#ESRA AYVAZ