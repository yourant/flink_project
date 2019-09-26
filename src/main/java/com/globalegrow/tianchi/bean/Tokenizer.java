package com.globalegrow.tianchi.bean;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.globalegrow.tianchi.constants.EventConstants;
import com.globalegrow.tianchi.event.AfContentId;
import com.globalegrow.tianchi.event.AfContentIds;
import com.globalegrow.tianchi.event.AfCreateOrderSuccess;
import com.globalegrow.tianchi.event.AfPurchase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


/**
 * @author zhougenggeng createTime  2019/9/26
 */
public class Tokenizer implements FlatMapFunction<AppDataModel, String> {
    @Override
    public void flatMap(AppDataModel appDataModel, Collector<String> collector) throws Exception {
        String platform = null;
        String countryCode = null;
        String eventTime = null;
        String eventDate = null;
        String intakeDate = DateUtil.today();
        String eventName = null;
        String eventValue = null;
        String sku = null;
        String skus = null;
        String createOrderPrices = null;
        String createOrderQuantitys = null;
        String purchaseOrderPrices = null;
        String purchaseOrderQuantitys = null;
        String[] createOrderPriceStrArr = null;
        String[] createOrderQuantityStrArr = null;
        Double[] createOrderPriceArr = null;
        Integer[] createOrderQuantityArr = null;
        String[] purchaseOrderPriceStrArr = null;
        String[] purchaseOrderQuantityStrArr = null;
        Double[] purchaseOrderPriceArr = null;
        Integer[] purchaseOrderQuantityArr = null;
        Integer skuExpCnt = 0;//商品被曝光数量
        Integer skuHitCnt = 0;//商品被点击数量
        Integer skuCartCnt = 0;//商品被加购物车数量
        Integer skuMarkedCnt = 0;//商品被加收藏数量
        Integer createOrderCnt = 0;//创建订单总数
        Integer purchaseOrderCnt = 0;//购买订单总数

        if(appDataModel.getPlatform() != null){
            platform = appDataModel.getPlatform();
        }
        if(appDataModel.getCountry_code() != null){
            countryCode = appDataModel.getCountry_code();
        }
        if(appDataModel.getEvent_time() != null){
            eventTime = appDataModel.getEvent_time();
            eventTime = DateUtil.format(DateUtil.date(Long.valueOf(eventTime)),"yyyy-MM-dd HH:mm:ss");
            eventDate = DateUtil.format(DateUtil.date(Long.valueOf(appDataModel.getEvent_time())),"yyyy-MM-dd");
        }
        if(appDataModel.getEvent_name() != null){
            eventName = appDataModel.getEvent_name();
        }
        if(appDataModel.getEvent_value() != null){
            eventValue = appDataModel.getEvent_value();
        }
        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_SKU_VIEW)){
            if(JSONUtil.isJsonObj(eventValue)){
                AfContentIds obj = JSONUtil.toBean(eventValue, AfContentIds.class);
                if(obj != null && StrUtil.isNotEmpty(obj.getAf_content_ids())){
                    skus = obj.getAf_content_ids();
                    skuExpCnt = 1;
                }else{
                    AfContentId afContentId = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = afContentId.getAf_content_id();
                    skuExpCnt = 1;
                }

            }
        }
        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_VIEW_PRODUCT)){
            if(JSONUtil.isJsonObj(eventValue)){
                AfContentIds obj = JSONUtil.toBean(eventValue, AfContentIds.class);
                if(obj != null && StrUtil.isNotEmpty(obj.getAf_content_ids())){
                    skus = obj.getAf_content_ids();
                    skuHitCnt = 1;
                }else{
                    AfContentId afContentId = JSONUtil.toBean(eventValue, AfContentId.class);
                    skus = afContentId.getAf_content_id();
                    skuHitCnt = 1;
                }
            }
        }
        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_ADD_TO_BAG)){
            if(JSONUtil.isJsonObj(eventValue)){
                AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                skus = obj.getAf_content_id();
                skuCartCnt = 1;
            }
        }
        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_ADD_TO_WISHLIST)){
            if(JSONUtil.isJsonObj(eventValue)){
                AfContentId obj = JSONUtil.toBean(eventValue, AfContentId.class);
                skus = obj.getAf_content_id();
                skuMarkedCnt = 1;
            }
        }

        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_CREATE_ORDER_SUCCESS)){
            if(JSONUtil.isJsonObj(eventValue)){
                AfCreateOrderSuccess obj = JSONUtil.toBean(eventValue, AfCreateOrderSuccess.class);
                skus = obj.getAf_content_id();
                if(obj.getAf_price() != null){
                    createOrderPrices = obj.getAf_price();
                }
                if(obj.getAf_quantity() != null){
                    createOrderQuantitys = obj.getAf_quantity();
                }
                createOrderCnt = 1;
            }
        }
        if(StrUtil.isNotBlank(eventName) && StrUtil.isNotBlank(eventValue) && eventName.equalsIgnoreCase(EventConstants.AF_PURCHASE)){
            if(JSONUtil.isJsonObj(eventValue)){
                try{
                    AfPurchase obj = JSONUtil.toBean(eventValue, AfPurchase.class);
                    skus = obj.getAf_content_id();
                    if(obj.getAf_price() != null){
                        purchaseOrderPrices = obj.getAf_price();
                    }
                    if(obj.getAf_quantity() != null){
                        purchaseOrderQuantitys = obj.getAf_quantity();
                    }
                    purchaseOrderCnt = 1;
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        if(StrUtil.isNotBlank(skus) && !skus.equalsIgnoreCase("null")){
            String[] skuArr = skus.toLowerCase().split(",");
            if(createOrderPrices != null){
                createOrderPriceStrArr = createOrderPrices.split(",");
                createOrderPriceArr =  Convert.toDoubleArray(createOrderPriceStrArr);
            }
            if(createOrderQuantitys != null){
                createOrderQuantityStrArr = createOrderQuantitys.split(",");
                createOrderQuantityArr = Convert.toIntArray(createOrderQuantityStrArr);
            }
            if(purchaseOrderPrices != null){
                purchaseOrderPriceStrArr = purchaseOrderPrices.split(",");
                purchaseOrderPriceArr =  Convert.toDoubleArray(purchaseOrderPriceStrArr);
            }
            if(purchaseOrderQuantitys != null){
                purchaseOrderQuantityStrArr = purchaseOrderQuantitys.split(",");
                purchaseOrderQuantityArr = Convert.toIntArray(purchaseOrderQuantityStrArr);
            }
            for (int i = 0; i < skuArr.length; i++) {
                sku = skuArr[i];
                Double gmv = 0.00;
                Double createOrderPrice = 0.00;
                Integer createOrderQuantity = 0;
                Double purchaseOrderAmount = 0.00;
                Double purchaseOrderPrice = 0.00;
                Integer purchaseOrderQuantity = 0;
                try {
                    if(createOrderPriceArr != null && createOrderPriceArr[i] != null){
                        createOrderPrice = createOrderPriceArr[i];
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    if(createOrderQuantityArr != null && createOrderQuantityArr[i] != null){
                        createOrderQuantity = createOrderQuantityArr[i];
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
                try {
                    if(purchaseOrderPriceArr != null && purchaseOrderPriceArr[i] != null){
                        purchaseOrderPrice = purchaseOrderPriceArr[i];
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    if(purchaseOrderQuantityArr != null && purchaseOrderQuantityArr[i] != null){
                        purchaseOrderQuantity = purchaseOrderQuantityArr[i];
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
                if (StrUtil.isNotEmpty(sku) && !sku.equalsIgnoreCase("null")
                    && StrUtil.isNotEmpty(platform) && !platform.equalsIgnoreCase("null")
                    && StrUtil.isNotEmpty(countryCode) && !countryCode.equalsIgnoreCase("null")
                    && StrUtil.isNotEmpty(eventName) && !eventName.equalsIgnoreCase("null")
                    && StrUtil.isNotEmpty(eventTime) && !eventTime.equalsIgnoreCase("null")) {
                    gmv = NumberUtil.mul(createOrderPrice,createOrderQuantity).doubleValue();
                    gmv = NumberUtil.round(gmv,2).doubleValue();
                    createOrderPrice = NumberUtil.round(createOrderPrice,2).doubleValue();
                    purchaseOrderAmount = NumberUtil.mul(purchaseOrderPrice,purchaseOrderQuantity).doubleValue();
                    purchaseOrderAmount = NumberUtil.round(purchaseOrderAmount,2).doubleValue();
                    purchaseOrderPrice = NumberUtil.round(purchaseOrderPrice,2).doubleValue();
                    String goods = new Goods(platform,countryCode,eventName,sku,eventTime,eventDate,intakeDate,createOrderPrice,createOrderQuantity,gmv,purchaseOrderPrice,purchaseOrderQuantity,purchaseOrderAmount,skuExpCnt,skuHitCnt,skuCartCnt,skuMarkedCnt,createOrderCnt,purchaseOrderCnt).toString();
                    collector.collect(goods);
                }
            }
        }
    }
}
