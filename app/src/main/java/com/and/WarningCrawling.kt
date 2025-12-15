package com.and

import com.google.gson.JsonParser
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.ResponseBody
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.GET
import retrofit2.http.Query

class WarningCrawling(private val recognizedTexts: MutableList<String>) {
    private val responseListMutex = Mutex()

    interface PublicDataService {
        @GET("/1471000/DURPrdlstInfoService03/getUsjntTabooInfoList03")
        fun getUsjntTabooInfoList(
            @Query("serviceKey") serviceKey: String,
            @Query("pageNo") pageNo: Int,
            @Query("numOfRows") numOfRows: Int,
            @Query("type") type: String,
            @Query("typeName") typeName: String,
            @Query("itemName") itemName: String
        ): Call<ResponseBody>
    }

    fun interface OnSuccessListener {
        fun onSuccessGetData(success: Boolean, productList: MutableList<String>, responseList: MutableMap<String, MutableList<String>>)
    }

    private val apiKey = BuildConfig.DataPortal_API_KEY
    private val productList: MutableList<String> = mutableListOf()
    private val responseList: MutableMap<String, MutableList<String>> = mutableMapOf()
    private var retrofit: Retrofit
    private var service: PublicDataService

    var onSuccessListener: OnSuccessListener? = null

    init {
        productList.apply {
            addAll(recognizedTexts)
        }

        retrofit = Retrofit.Builder()
            .baseUrl("https://apis.data.go.kr")
            .addConverterFactory(GsonConverterFactory.create())
            .build()

        service = retrofit.create(PublicDataService::class.java)
    }

    fun getWarningDrug() {
        val jobs = mutableListOf<Job>()

        val errorHandler = CoroutineExceptionHandler { _, exception ->
            onSuccessListener?.onSuccessGetData(false, productList, responseList)
        }

        CoroutineScope(Dispatchers.IO + errorHandler).launch {
            productList.forEach { productName ->
                responseList[productName] = mutableListOf()

                jobs += launch {
                    for (page in 1..8) {
                        val response = service.getUsjntTabooInfoList(
                            serviceKey = apiKey,
                            pageNo = page,
                            numOfRows = 100,
                            type = "json",
                            typeName = "병용금기",
                            itemName = productName
                        ).execute()

                        if (response.isSuccessful) {
                            val responseBody = response.body()?.string()
                            val continueProcessing = parseAndAddProductNames(responseBody, productName)
                            response.body()?.close()

                            if (!continueProcessing) break
                        }
                    }
                }
            }

            jobs.joinAll()

            if (isActive) {
                withContext(Dispatchers.Main) {
                    onSuccessListener?.onSuccessGetData(true, productList, responseList)
                }
            }
        }
    }

    private suspend fun parseAndAddProductNames(responseBody: String?, productName: String): Boolean {
        responseBody?.let {
            val jsonObject = JsonParser.parseString(it).asJsonObject
            val bodyObject = jsonObject.getAsJsonObject("body")
            val items = bodyObject.getAsJsonArray("items")

            val productNames = mutableListOf<String>()
            if (items == null) {
                return false
            }
            
            for (item in items) {
                val mainIngr = item.asJsonObject.get("MIXTURE_ITEM_NAME").asString
                productNames.add(mainIngr)
            }

            responseListMutex.withLock {
                responseList[productName]?.addAll(productNames.distinct())
            }
        }

        return true
    }
}