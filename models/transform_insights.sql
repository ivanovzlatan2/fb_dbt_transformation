{{ config(materialized='table') }}


with insights as (
   select * from development_test.fb_test_3_ads_insights
),

base as (
--this unnests the actions values to provide one row per day per action type
   select
       date(date_start) as date,
       nullif(campaign_id,'') as campaign_id,
       nullif(ad_id,'') as ad_id,
       nullif(adset_id,'') as adset_id,
			 _airbyte_emitted_at,
       ad_name,
       adset_name,
       campaign_name,
       account_name,
       cpc,
       cpm,
       ctr,
       frequency,
       impressions,
       inline_link_clicks,
       reach,
       objective,
       spend,
       _1d_click,
       _7d_click,
       _28d_click,
       value,
       nullif(action_type,'') as action_type,
       nullif(action_destination,'') as action_destination
   from insights
   cross join unnest(insights.actions)
),

basic_report_actions as (select date, ad_id,_airbyte_emitted_at,campaign_id, adset_id, ad_name, adset_name,account_name, campaign_name, cpc, cpm, ctr, frequency, impressions,inline_link_clicks,reach,objective,spend,
   any_value(if(action_type = "view_content", value, null)) as view_content,
   any_value(if(action_type = "omni_add_to_cart", value, null)) as add_to_cart,
   any_value(if(action_type = "omni_initiated_checkout", value, null)) as initiated_checkout,
   any_value(if(action_type = "omni_purchase", value, null)) as purchase,
   any_value(if(action_type = "omni_purchase", _1d_click, null)) as purchase_1dc,
   any_value(if(action_type = "omni_purchase", _7d_click, null)) as purchase_7dc,
   any_value(if(action_type = "omni_purchase", _28d_click, null)) as purchase_28dc,
   any_value(if(action_type = "landing_page_view", value, null)) as landing_page_view,
   any_value(if(action_type = "post_reaction", value, null)) as post_reaction,
   any_value(if(action_type = "comment", value, null)) as comment,
   any_value(if(action_type = "lead", value, null)) as lead,
   any_value(if(action_type = "onsite_conversion.messaging_first_reply", value, null)) as messages,
   any_value(if(action_type = "onsite_conversion.messaging_conversation_started_7d", value, null)) as messages_7d,
   any_value(if(action_type = "initiate_checkout", value, null)) as initiate_checkout,
   any_value(if(action_type = "complete_registration", value, null)) as complete_registration,
from base
group by base.ad_id, base.campaign_id,base._airbyte_emitted_at,base.adset_id,base.date, ad_name, adset_name,account_name,campaign_name, cpc, cpm, ctr, frequency, impressions,inline_link_clicks,reach,objective,spend
order by base.date ASC),

action_values as (
with insights as (
   select * from development_test.fb_test_3_ads_insights
),

action_values as (
--this unnests the actions values to provide one row per day per action type
   select
       date(date_start) as date,
       nullif(campaign_id,'') as campaign_id,
       nullif(ad_id,'') as ad_id,
       nullif(adset_id,'') as adset_id,
       value,
       _1d_click,
       _7d_click,
       _28d_click,       
       nullif(action_type,'') as action_type,
       nullif(action_destination,'') as action_destination
   from insights
   cross join unnest(insights.action_values)
)

select date, ad_id,
   any_value(if(action_type = "omni_add_to_cart", value, null)) as add_to_cart_value,
   any_value(if(action_type = "omni_purchase", value, null)) as purchase_value,
   any_value(if(action_type = "omni_purchase", _1d_click, null)) as purchase_value_1dc,
   any_value(if(action_type = "omni_purchase", _7d_click, null)) as purchase_value_7dc,
   any_value(if(action_type = "omni_purchase", _28d_click, null)) as purchase_value_28dc,
from action_values
group by action_values.ad_id,action_values.date

),

basic_report_final as (
select basic_report_actions.date as date, basic_report_actions.ad_id, _airbyte_emitted_at, campaign_name,adset_name, ad_name,account_name,  adset_id, campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,purchase_1dc,purchase_7dc,purchase_28dc,purchase_value_1dc,purchase_value_7dc,purchase_value_28dc,landing_page_view, purchase_value, comment,lead, messages, messages_7d, initiate_checkout, complete_registration from basic_report_actions
left join action_values
ON basic_report_actions.date=action_values.date AND basic_report_actions.ad_id=action_values.ad_id
),

current_campaign_name as (
select * from (
  select
    date,
    campaign_name,
    campaign_id,
    
    row_number() over(partition by campaign_id order by date desc) as rn
    from basic_report_final
    ) t
    where t.rn = 1
),

merge_campaign_names as (
select basic_report_final.date as date, basic_report_final.ad_id, _airbyte_emitted_at, basic_report_final.campaign_name,basic_report_final.adset_name, ad_name,basic_report_final.account_name,  adset_id, basic_report_final.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,purchase_1dc,purchase_7dc,purchase_28dc,purchase_value_1dc,purchase_value_7dc,purchase_value_28dc,landing_page_view, purchase_value, comment,lead, messages, messages_7d, initiate_checkout, complete_registration, current_campaign_name.campaign_name as current_campaign_name from basic_report_final
left join current_campaign_name
ON basic_report_final.campaign_id=current_campaign_name.campaign_id
),

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

merge_adset_names as (
select merge_campaign_names.date as date, merge_campaign_names.ad_id,_airbyte_emitted_at, merge_campaign_names.campaign_name,merge_campaign_names.adset_name, ad_name,merge_campaign_names.account_name,  merge_campaign_names.adset_id, merge_campaign_names.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,purchase_1dc,purchase_7dc,purchase_28dc,purchase_value_1dc,purchase_value_7dc,purchase_value_28dc,landing_page_view, purchase_value, comment,lead, messages, messages_7d, initiate_checkout, complete_registration,current_campaign_name,current_adset_name.adset_name as current_adset_name from merge_campaign_names
left join current_adset_name
ON merge_campaign_names.adset_id=current_adset_name.adset_id
),

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

merge_ad_names as (
select merge_adset_names.date as date, merge_adset_names.ad_id, _airbyte_emitted_at, merge_adset_names.campaign_name,merge_adset_names.adset_name, merge_adset_names.ad_name,merge_adset_names.account_name,  adset_id, merge_adset_names.campaign_id, cpc, cpm, ctr, frequency, impressions, inline_link_clicks, reach,objective, spend, view_content, add_to_cart,add_to_cart_value,purchase,purchase_1dc,purchase_7dc,purchase_28dc,purchase_value_1dc,purchase_value_7dc,purchase_value_28dc,landing_page_view, purchase_value, comment,lead, messages, messages_7d, initiate_checkout, complete_registration,current_campaign_name,merge_adset_names.current_adset_name, current_ad_name.ad_name as current_ad_name from merge_adset_names
left join current_ad_name
ON merge_adset_names.ad_id=current_ad_name.ad_id
)


select * from (
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
    purchase_1dc,purchase_7dc,purchase_28dc,purchase_value_1dc,purchase_value_7dc,purchase_value_28dc,
    landing_page_view,
    purchase_value,
    comment,
    lead, 
    messages,
    messages_7d, 
    initiate_checkout, 
    complete_registration,
    current_campaign_name,
    current_adset_name,
    current_ad_name,
    
    row_number() over(partition by ad_id, date order by _airbyte_emitted_at desc) as rn
    from merge_ad_names
    ) t
    where t.rn = 1
