table! {
    #[allow(non_snake_case)]
    Properties (id) {
        id -> Char,
        name -> Varchar,
        adPlatformId -> Char,
        accountId -> Nullable<Varchar>,
        campaignId -> Nullable<Char>,
        adSetId -> Nullable<Char>,
        adId -> Nullable<Char>,
        createdAt -> Datetime,
        updatedAt -> Datetime,
        propertyType -> Varchar,
        propertyId -> Varchar,
        bidStrategy -> Nullable<Varchar>,
        bidAmount -> Nullable<Double>,
        dailyBudget -> Nullable<Double>,
        externalCreatedAt -> Nullable<Datetime>,
        propertyStatus -> Nullable<Varchar>,
        weeklyFrequency -> Nullable<Double>,
        ltv30 -> Nullable<Double>,
        displayStatus -> Nullable<Varchar>,
        objective -> Nullable<Varchar>,
        handle -> Nullable<Varchar>,
    }
}

table! {
    #[allow(non_snake_case)]
    UpperFunnelMetricFields (id) {
        id -> Char,
        name -> Varchar,
        organizationId -> Char,
        hasCurrency -> Bool,
        createdAt -> Datetime,
        updatedAt -> Datetime,
        isLowerFunnel -> Nullable<Bool>,
        calculationMode -> Nullable<Varchar>,
        attributionMode -> Nullable<Varchar>,
        attributionWindow -> Nullable<Integer>,
        lowerFunnelMetricName -> Nullable<Varchar>,
    }
}

table! {
    #[allow(non_snake_case)]
    UpperFunnelMetricValues (id) {
        date -> Date,
        upperFunnelMetricFieldId -> Char,
        propertyId -> Char,
        thirdPartyServiceConnectionId -> Char,
        value -> Nullable<Double>,
        sourceValue -> Nullable<Double>,
        sourceCurrency -> Nullable<Varchar>,
        targetingType -> Nullable<Varchar>,
        targetingValue -> Nullable<Varchar>,
        createdAt -> Datetime,
        updatedAt -> Datetime,
        id -> Char,
        subAdPlatform -> Nullable<Varchar>,
        adPlatform -> Varchar,
    }
}

joinable!(UpperFunnelMetricValues -> Properties (propertyId));
joinable!(UpperFunnelMetricValues -> UpperFunnelMetricFields (upperFunnelMetricFieldId));

allow_tables_to_appear_in_same_query!(Properties, UpperFunnelMetricFields, UpperFunnelMetricValues,);
