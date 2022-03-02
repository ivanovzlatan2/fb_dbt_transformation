{{ config(materialized='table') }}


with insights as (
  select * from  devteam.ads_insights
),


-- Actions are already normalized from Airbyte in ..._ads_insights_actions. 
-- This query extracts the necessary "action_types" from ..._ads_insights_actions by extracting the necessary "action_types", then sets them as columns and assign the corresponding value as the metric.

actions as (
  select 
     _airbyte_ads_insights_hashid as actions_id,
     any_value(if(action_type = "offsite_conversion.fb_pixel_view_content", value, null)) as view_content,
     any_value(if(action_type = "offsite_conversion.fb_pixel_add_to_cart", value, null)) as add_to_cart,
     any_value(if(action_type = "offsite_conversion.fb_pixel_initiate_checkout", value, null)) as initiate_checkout,
     any_value(if(action_type = "offsite_conversion.fb_pixel_purchase", value, null)) as purchase,
     any_value(if(action_type = "landing_page_view", value, null)) as landing_page_view,
     any_value(if(action_type = "comment", value, null)) as comment,
     any_value(if(action_type = "complete_registration", value, null)) as complete_registration,
  from devteam.ads_insights_actions
  group by actions_id
),

-- Action_Values are already normalized from Airbyte in ..._ads_insights_action_values. 
-- This query extracts the necessary "action_types" from ..._ads_insights_action_values, then sets them as columns and assigns the corresponding value as the metric.

action_values as (
  select 
     _airbyte_ads_insights_hashid as action_values_id,
     any_value(if(action_type = "offsite_conversion.fb_pixel_view_content", value, null)) as view_content_value,
     any_value(if(action_type = "offsite_conversion.fb_pixel_add_to_cart", value, null)) as add_to_cart_value,
     any_value(if(action_type = "offsite_conversion.fb_pixel_initiate_checkout", value, null)) as initiate_checkout_value,
     any_value(if(action_type = "offsite_conversion.fb_pixel_purchase", value, null)) as purchase_value,
  from devteam.ads_insights_action_values
  group by action_values_id
),


-- This query joins insights together with actions and action values on their "hashid". Only selected and necessary columns are specified.

basic_report as (
  select 
    date_start, ad_id, _airbyte_emitted_at, campaign_name,adset_name, ad_name,account_name,  adset_id, campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,purchase_value,landing_page_view, comment, initiate_checkout, complete_registration, 
  from insights
  left join actions
  ON insights._airbyte_ads_insights_hashid=actions.actions_id
  left join  action_values
  ON insights._airbyte_ads_insights_hashid=action_values.action_values_id
),

-- Campaign_name is extracted from FB API and is subject to change on a daily basis, because it serves advertisers an annotation purpose.
-- This query's result is producing a list of most recent campaign_names, partitioned by campaign_id.

current_campaign_name as (
select * from (
  select
    date_start,
    campaign_name,
    campaign_id,
    
    row_number() over(partition by campaign_id order by date_start desc) as rn
    from basic_report
    ) t
    where t.rn = 1
),

-- This query joins all previous data together with "current_campaign_name" as an additional column. 
-- The "campaign_name" data is staying the same as when extracted.

merge_campaign_names as (
select basic_report.date_start as date, basic_report.ad_id, _airbyte_emitted_at, basic_report.campaign_name,basic_report.adset_name, ad_name,basic_report.account_name,  adset_id, basic_report.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,landing_page_view, purchase_value, comment,initiate_checkout, complete_registration, current_campaign_name.campaign_name as current_campaign_name from basic_report
left join current_campaign_name
ON basic_report.campaign_id=current_campaign_name.campaign_id
),

-- Adset_name is extracted from FB API and is subject to change on a daily basis, because it serves advertisers an annotation purpose.
-- This query's result is producing a list of most recent adset_names, partitioned by adset_id.

current_adset_name as (
select * from (
  select
    date,
    adset_name,
    adset_id,
    
    row_number() over(partition by adset_id order by date desc) as rn
    from merge_campaign_names
    ) t
    where t.rn = 1
),

-- This query joins all previous data together with "current_adset_name" as an additional column. 
-- The "adset_name" data is staying the same as when extracted.

merge_adset_names as (
select merge_campaign_names.date as date, merge_campaign_names.ad_id,_airbyte_emitted_at, merge_campaign_names.campaign_name,merge_campaign_names.adset_name, ad_name,merge_campaign_names.account_name,  merge_campaign_names.adset_id, merge_campaign_names.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,landing_page_view, purchase_value, comment, initiate_checkout, complete_registration,current_campaign_name,current_adset_name.adset_name as current_adset_name from merge_campaign_names
left join current_adset_name
ON merge_campaign_names.adset_id=current_adset_name.adset_id
),

-- Ad_name is extracted from FB API and is subject to change on a daily basis, because it serves advertisers an annotation purpose.
-- This query's result is producing a list of most recent ad_names, partitioned by ad_id.

current_ad_name as (
select * from (
  select
    date,
    ad_name,
    ad_id,
    
    row_number() over(partition by ad_id order by date desc) as rn
    from merge_adset_names
    ) t
    where t.rn = 1
),

-- This query joins all previous data together with "current_ad_name" as an additional column. 
-- The "ad_name" data is staying the same as when extracted.

merge_ad_names as (
select merge_adset_names.date as date, merge_adset_names.ad_id, _airbyte_emitted_at, merge_adset_names.campaign_name,merge_adset_names.adset_name, merge_adset_names.ad_name,merge_adset_names.account_name,  adset_id, merge_adset_names.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,landing_page_view, purchase_value, comment, initiate_checkout, complete_registration,current_campaign_name,merge_adset_names.current_adset_name, current_ad_name.ad_name as current_ad_name from merge_adset_names
left join current_ad_name
ON merge_adset_names.ad_id=current_ad_name.ad_id
)

-- This query produces the table with the name "transform_insights" with all the necessary columns and data, current names and rows are filtered with impressions > 0 OR purchases > 0, because otherwise it produces empty rows of campaigns that have zero metric values.


select * except(rn) from (
  select
    date,
    ad_id,
    campaign_name,
    adset_name,
    ad_name,
    account_name,
    adset_id,
    campaign_id,
    cpc,
    cpm,
    ctr,
    frequency,
    impressions,
    inline_link_clicks,
    reach,
    objective,
    spend,
    view_content,
    add_to_cart,
    add_to_cart_value,
    purchase,
    landing_page_view,
    purchase_value,
    comment,
    initiate_checkout, 
    complete_registration,
    current_campaign_name,
    current_adset_name,
    current_ad_name,
    
    row_number() over(partition by ad_id, date order by _airbyte_emitted_at desc) as rn
    from merge_ad_names
    ) t
    where t.rn = 1 AND spend != 0 OR purchase != 0
