# 🛠️ Duplicate Key Cleaner Console App Usage Guide

এই ডকুমেন্টে Console App ব্যবহার করে কিভাবে Duplicate Key detect, analyze এবং migration করা হবে তা step-by-step বর্ণনা করা হয়েছে।

---

## ✅ Step 1: Data Export & Import

প্রথমে নিচের site থেকে data export করতে হবে:

👉 https://cloud.seliseblocks.com/

তারপর export করা data **MongoDB এর `Raw_Keys` collection এ import করতে হবে।**

---

## ✅ Step 2: Duplicate Report Generate

নিচের method call করলে duplicate report generate হবে:

```csharp
await DuplicateDetector.GenerateDuplicateReport();
```

এটি Duplicate_Key_Report collection এ structured format এ data store করবে।
```json
{
  "KeyName": "CUSTOMER",
  "TotalDuplicateCount": 5,
  "HasRootModule": false,
  "HasGenericModule": true,
  "IsConsistent": false
}
```
👉 এই structure এর মাধ্যমে আমরা সহজে বুঝতে পারবো:

Root module আছে কিনা
Generic module আছে কিনা
Data consistent কিনা

## ✅ Step 3: Report Analyze করা

Report দেখার জন্য নিচের method call করা যাবে:

```csharp
await TestDuplicateReportAnalyzer.GenerateSummaryReport(
    keyNameFilter: null,
    hasRootModule: true,
    hasGenericModule: true,
    isConsistent: false,
    isDeletePermission: false
);
```

🔄 Migration Strategy

আমরা data migration দুইভাবে করবো:

1. Consistent Data
2. Inconsistent Data

## ✅ Step 4: Consistent Data Migration

প্রথমে Consistent data এর উপর migration চালানো হবে।

👉 এখানে ৪টি filter ব্যবহার করা হবে:

- hasRootModule
- hasGenericModule
- isConsistent
- isDeletePermission

```csharp
await DuplicateReportAnalyzer.GenerateSummaryReport(
    keyNameFilter: "SALUTATION_MS",
    hasRootModule: true,
    hasGenericModule: false,
    isConsistent: true,
    isDeletePermission: true
);
```

🎯 Logic:

- শুধুমাত্র generic-app module রাখা হবে
- যদি না থাকে → নতুন করে add করা হবে
- বাকি সব module delete করা হবে

## ✅ Step 5: Inconsistent Data Report

Inconsistent migration এর আগে report দেখে নেওয়া যাবে:
```csharp
await InconsistentDuplicateCleaner.GenerateSummaryReport(
    keyNameFilter: null,
    hasRootModule: false,
    hasGenericModule: false,
    isConsistent: false,
    isDeletePermission: false
);
```

## ✅ Step 6: Inconsistent Data Migration

👉 এখানে আমরা একেকটা filter ধরে migration চালাবো

🔹 Important Rule:
- Match Group → Migration হবে
- Unmatch Group → কোন migration হবে না

🔍 Filter Based Logic

### 🔸 Case: B & D Filter (HasGenericModule = true)

  - Generic App reference হিসেবে ব্যবহার করা হবে

  - Match group এ: Generic app এর সাথে match করে এমন data রাখা হবে
  - Unmatch group:সব data রাখা হবে (delete করা হবে না)

👉 এরপর:

   - Match group থেকে generic-app ছাড়া বাকি সব module delete করা হবে
   - Unmatch group untouched থাকবে

### 🔸 Case: C & A Filter (HasGenericModule = false)
 - Generic app module নেই
 - যদি hasRootModule = true হয় → app-root reference হিসেবে ব্যবহার করা হবে

#### 🎯 Logic:
- Match & Unmatch group identify করা হবে
এরপর decision নেওয়া হবে:
#### 👉 Case 1: Match Count বেশি হলে
- Match group এর সব module delete
নতুন করে generic-app add করা হবে
- Unmatch group untouched থাকবে

### 👉 Case 2: Unmatch Count বেশি হলে
- Unmatch group এর মধ্যে common match খোঁজা হবে
- যদি common match বেশি হয়:
সেই data delete করা হবে
নতুন generic-app add করা হবে
অন্য সব data untouched থাকবে

#### 👉 একই logic A Filter এর জন্যও apply হবে

🧾 Summary
- Consistent data → সহজভাবে clean করা হয়
- Inconsistent data → smart decision based migration
- Unmatch group → সবসময় safe রাখা হয় (no delete)

✅ এই process follow করলে safe ভাবে duplicate key clean করা যাবে without data loss.
