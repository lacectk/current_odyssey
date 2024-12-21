"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[4030],{60648:function(e,t,n){n.d(t,{Z:function(){return m}});var i=n(52322),s=n(43212),r=n(14934),a=n(55010),o=n(2784),l=n(90797),d=n(88257),c=n(54516),u=n(84046),x=n(59351),h=n(33182),p=n(88610);let m=(0,o.memo)(e=>{let{name:t,run:n,showHover:m=!1,showButton:j=!0,showSummary:g=!0}=e,{status:f}=n,y=(0,o.useMemo)(()=>{switch(f){case d.VOA.SUCCESS:return"success";case d.VOA.CANCELED:case d.VOA.CANCELING:case d.VOA.FAILURE:return"danger";default:return"none"}},[f]);return(0,i.jsxs)(r.x,{flex:{direction:"row",justifyContent:"space-between",alignItems:"flex-start",gap:16},children:[(0,i.jsxs)(r.x,{flex:{direction:"column",alignItems:"flex-start",gap:4},children:[(0,i.jsxs)(r.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[(()=>{let e=(0,i.jsx)(s.V,{intent:y,children:(0,i.jsxs)(r.x,{flex:{direction:"row",alignItems:"center",gap:4},children:[(0,i.jsx)(c.K9,{status:n.status,size:10}),(0,i.jsx)(h.Zr,{run:n})]})});return m?(0,i.jsx)(a.J,{position:"top",interactionKind:"hover",content:(0,i.jsx)("div",{children:(0,i.jsx)(u.qL,{run:n,name:t})}),hoverOpenDelay:100,children:e}):e})(),g?(0,i.jsx)(h.Tw,{run:n}):null]}),g&&(x.hd.has(n.status)||x.Ys.has(n.status))?(0,i.jsx)(l.I,{runId:n.id}):void 0]}),j?(0,i.jsx)(p.A,{to:`/runs/${n.id}`,children:"View run"}):null]})})},90797:function(e,t,n){n.d(t,{I:function(){return h}});var i=n(52322),s=n(35292),r=n(39013),a=n(19126),o=n.n(a),l=n(2784),d=n(47933),c=n(20958),u=n(88257),x=n(59351);let h=e=>{let{runId:t}=e,{data:n}=(0,c.aM)(p,{variables:{runId:t}}),a=n?.pipelineRunOrError,h=a?.__typename==="Run"?a.status:null,m=(0,l.useMemo)(()=>{if(a?.__typename!=="Run")return[];let{status:e}=a;return x.hd.has(e)?a.stepStats.filter(e=>e.status===u.SGm.FAILURE):x.Ys.has(e)?a.stepStats.filter(e=>e.status===u.SGm.IN_PROGRESS):[]},[a]),j=m.length;if(!j||!h)return null;if(x.hd.has(h)){if(1===j){let e=m[0],n=e.endTime?o().stringify({focusedTime:Math.floor(1e3*e.endTime)},{addQueryPrefix:!0}):"";return(0,i.jsxs)(s.YS,{color:r.$(),children:["Failed at ",(0,i.jsx)(d.rU,{to:`/runs/${t}${n}`,children:e.stepKey})]})}return(0,i.jsxs)(s.YS,{color:r.$(),children:["Failed at ",(0,i.jsxs)(d.rU,{to:`/runs/${t}`,children:[j," steps"]})]})}if(x.Ys.has(h)){if(1===j){let e=m[0],n=e.endTime?o().stringify({focusedTime:Math.floor(1e3*e.endTime)},{addQueryPrefix:!0}):"";return(0,i.jsxs)(s.YS,{color:r.$(),children:["In progress at ",(0,i.jsx)(d.rU,{to:`/runs/${t}${n}`,children:e.stepKey})]})}return(0,i.jsxs)(s.YS,{color:r.$(),children:["In progress at ",(0,i.jsxs)(d.rU,{to:`/runs/${t}`,children:[j," steps"]})]})}return null},p=(0,c.Ps)`
  query StepSummaryForRunQuery($runId: ID!) {
    pipelineRunOrError(runId: $runId) {
      ... on Run {
        id
        status
        stepStats {
          endTime
          stepKey
          status
        }
      }
    }
  }
`},77837:function(e,t,n){n.d(t,{Jd:function(){return g},dY:function(){return f},i0:function(){return j}});var i=n(52322),s=n(14934),r=n(79106),a=n(37483),o=n(34147),l=n(39013),d=n(35292),c=n(35002),u=n(36853),x=n(20958),h=n(78491),p=n(71902),m=n(30916);let j=e=>{let{runIds:t}=e,{data:n,loading:o}=(0,x.aM)(y,{variables:{filter:{runIds:t}}});return o||!n?(0,i.jsx)(s.x,{padding:32,children:(0,i.jsx)(r.$,{purpose:"section"})}):"Runs"!==n.pipelineRunsOrError.__typename?(0,i.jsx)(s.x,{padding:32,children:(0,i.jsx)(a.t,{icon:"error",title:"An error occurred",description:n.pipelineRunsOrError.message})}):(0,i.jsx)(s.x,{padding:{bottom:8},children:(0,i.jsx)(p.A,{runs:n.pipelineRunsOrError.results})})},g=e=>{let{originRunIds:t}=e;return t&&t.length?(0,i.jsxs)(o.Z,{direction:"column",spacing:16,children:[(0,i.jsxs)(s.x,{padding:12,border:{side:"bottom",color:l.Aw()},children:[(0,i.jsxs)(d.uT,{children:["Targeted Runs",(0,i.jsx)(c.u,{content:"Runs this tick reacted on and reported back to.",children:(0,i.jsx)(u.JO,{name:"info",color:l.$()})})]}),(0,i.jsx)(j,{runIds:t})]}),(0,i.jsxs)(s.x,{padding:12,margin:{bottom:8},children:[(0,i.jsxs)(d.uT,{children:["Requested Runs",(0,i.jsx)(c.u,{content:"Runs launched by the run requests in this tick.",children:(0,i.jsx)(u.JO,{name:"info",color:l.$()})})]}),(0,i.jsx)(a.t,{icon:"sensors",title:"No runs to display",description:"This sensor does not target a pipeline or job."})]})]}):null},f=(0,x.Ps)`
  fragment TickTagFragment on InstigationTick {
    id
    status
    timestamp
    skipReason
    runIds
    runKeys
    error {
      ...PythonErrorFragment
    }
  }

  ${h.B}
`,y=(0,x.Ps)`
  query LaunchedRunListQuery($filter: RunsFilter!) {
    pipelineRunsOrError(filter: $filter, limit: 500) {
      ... on PipelineRuns {
        results {
          ...RunTableRunFragment
          id
        }
      }
      ... on InvalidPipelineRunsFilterError {
        message
      }
      ...PythonErrorFragment
    }
  }

  ${m.Z}
  ${h.B}
`},581:function(e,t,n){n.d(t,{Jy:function(){return g},RX:function(){return p},S9:function(){return m},ai:function(){return j},vw:function(){return y}});var i=n(52322),s=n(34147),r=n(35292),a=n(39013),o=n(47933),l=n(2728),d=n(77837),c=n(20958),u=n(78491);n(60648);var x=n(54516),h=n(33182);let p=e=>{let{run:t}=e;return(0,i.jsxs)(s.Z,{direction:"row",spacing:4,alignItems:"center",children:[(0,i.jsx)(x.K9,{status:t.status}),(0,i.jsx)(o.rU,{to:`/runs/${t.id}`,target:"_blank",rel:"noreferrer",children:(0,i.jsx)(r.fv,{children:(0,h.EI)({id:t.id})})})]})},m=(0,c.Ps)`
  fragment RunStatusFragment on Run {
    id
    status
  }
`,j=(0,c.Ps)`
  fragment InstigationStateFragment on InstigationState {
    id
    selectorId
    name
    instigationType
    status
    hasStartPermission
    hasStopPermission
    repositoryName
    repositoryLocationName
    typeSpecificData {
      ... on SensorData {
        lastRunKey
        lastCursor
      }
      ... on ScheduleData {
        cronSchedule
      }
    }
    runs(limit: 1) {
      id
      ...RunStatusFragment
      ...RunTimeFragment
    }
    status
    ticks(limit: 1) {
      id
      cursor
      ...TickTagFragment
    }
    runningCount
  }

  ${m}
  ${h.$X}
  ${d.dY}
`,g=l.ZP.table.withConfig({componentId:"sc-2b645072-0"})(["font-size:13px;border-spacing:0;&&&&& tr{box-shadow:none;}&&&&& tbody > tr > td{background:transparent;box-shadow:none !important;padding:1px 0;}&&&&& tbody > tr > td:first-child{color:",";}"],a.$()),f=(0,c.Ps)`
  fragment DynamicPartitionsRequestResultFragment on DynamicPartitionsRequestResult {
    partitionsDefName
    partitionKeys
    skippedPartitionKeys
    type
  }
`,y=(0,c.Ps)`
  fragment HistoryTick on InstigationTick {
    id
    tickId
    status
    timestamp
    endTimestamp
    cursor
    instigationType
    skipReason
    requestedAssetMaterializationCount
    runIds
    runs {
      id
      status
      ...RunStatusFragment
    }
    originRunIds
    error {
      ...PythonErrorFragment
    }
    logKey
    ...TickTagFragment
    dynamicPartitionsRequestResults {
      ...DynamicPartitionsRequestResultFragment
    }
  }
  ${m}
  ${u.B}
  ${d.dY}
  ${f}
`},44030:function(e,t,n){n.d(t,{X:function(){return w},c:function(){return k}});var i=n(52322);n(26529);var s=n(92506),r=n(38491),a=n(14934),o=n(45774),l=n(37483),d=n(35292),c=n(43212),u=n(89891),x=n(1747),h=n(53664),p=n(2784),m=n(77837),j=n(581),g=n(5620),f=n(20958),y=n(8068),v=n(78491),I=n(99590),R=n(71406),S=n(22791),b=n(88257),T=n(86269);let w=e=>{let{tickId:t,tickResultType:n,isOpen:a,instigationSelector:o,onClose:l}=e;return(0,i.jsxs)(s.Vq,{isOpen:a,onClose:l,style:{width:"80vw",maxWidth:"1200px",minWidth:"600px"},children:[(0,i.jsx)(E,{tickId:t,tickResultType:n,instigationSelector:o}),(0,i.jsx)(s.cN,{topBorder:!0,children:(0,i.jsx)(r.zx,{onClick:l,children:"Close"})})]})},E=e=>{let{tickId:t,tickResultType:n,instigationSelector:r}=e,{data:c,loading:u}=(0,f.aM)(P,{variables:{instigationSelector:r,tickId:t||""},skip:!t}),x=c?.instigationStateOrError.__typename==="InstigationState"?c?.instigationStateOrError.tick:void 0,[h,j]=(0,p.useMemo)(()=>[x?.dynamicPartitionsRequestResults.filter(e=>e.type===b.Dq2.ADD_PARTITIONS&&e.partitionKeys?.length),x?.dynamicPartitionsRequestResults.filter(e=>e.type===b.Dq2.DELETE_PARTITIONS&&e.partitionKeys?.length)],[x?.dynamicPartitionsRequestResults]);return u?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(s.fK,{label:`Tick for ${r.name}`}),(0,i.jsx)(a.x,{style:{padding:64},flex:{alignItems:"center",justifyContent:"center"},children:(0,i.jsx)(o.f,{label:"Loading tick details…"})})]}):x?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(s.fK,{label:(0,i.jsxs)(i.Fragment,{children:[(0,i.jsxs)("span",{children:["Tick for ",r.name,": "]}),(0,i.jsx)(T.v,{timestamp:x.timestamp,timeFormat:{showTimezone:!1,showSeconds:!0}})]})}),(0,i.jsx)(a.x,{padding:{vertical:12,horizontal:24},border:"bottom",children:(0,i.jsx)(k,{tick:x,tickResultType:n})}),"materializations"===n?(0,i.jsx)(g.u,{tick:x}):null,"runs"===n?(0,i.jsxs)("div",{style:{height:"500px",overflowY:"auto"},children:[x.runIds.length?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(a.x,{padding:{vertical:16,horizontal:24},border:"bottom",children:(0,i.jsx)(d.Tj,{children:"Requested runs"})}),(0,i.jsx)(m.i0,{runIds:x.runIds})]}):x.originRunIds.length?(0,i.jsx)(m.Jd,{originRunIds:x.originRunIds}):null,h?.length?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(a.x,{padding:{vertical:12,horizontal:24},border:"bottom",children:(0,i.jsx)(d.Tj,{children:"Added partitions"})}),(0,i.jsx)($,{partitions:h})]}):null,j?.length?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(a.x,{padding:{vertical:12,horizontal:24},border:"bottom",children:(0,i.jsx)(d.Tj,{children:"Deleted partitions"})}),(0,i.jsx)($,{partitions:j})]}):null,x.error?(0,i.jsx)(a.x,{padding:24,children:(0,i.jsx)(I.T9,{error:x.error})}):null,x.skipReason?(0,i.jsxs)(a.x,{padding:24,children:[(0,i.jsx)("strong",{children:"Skip reason:"})," ",x.skipReason]}):null]}):null]}):(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(s.fK,{label:`Tick for ${r.name}`}),(0,i.jsx)(a.x,{style:{padding:64},flex:{alignItems:"center",justifyContent:"center"},children:(0,i.jsx)(l.t,{icon:"no-results",title:"Tick details not found",description:"Details for this tick could not be found."})})]})};function k(e){let{tick:t,tickResultType:n}=e,s=(0,p.useMemo)(()=>{switch(t?.status){case b.izS.FAILURE:return"danger";case b.izS.STARTED:return"primary";case b.izS.SUCCESS:return"success"}},[t]);return(0,i.jsx)(i.Fragment,{children:(0,i.jsxs)("div",{style:{display:"grid",gridTemplateColumns:"repeat(3, minmax(0, 1fr))",gap:12},children:[(0,i.jsxs)(a.x,{flex:{direction:"column",gap:4},children:[(0,i.jsx)(d.Tj,{children:"Status"}),(0,i.jsxs)(a.x,{flex:{direction:"row",gap:4,alignItems:"center"},children:[(0,i.jsx)(c.V,{intent:s,children:t.status===b.izS.STARTED?"Evaluating…":(0,i.jsxs)(i.Fragment,{children:[("materializations"!==n&&"runIds"in t?t.runIds.length:t.requestedAssetMaterializationCount)??0," ","requested"]})}),t.error?(0,i.jsx)(u.Z,{onClick:()=>{(0,y._K)({title:"Tick error",body:(0,i.jsx)(I.T9,{error:t.error})})},children:"View error"}):null]})]}),(0,i.jsxs)(a.x,{flex:{direction:"column",gap:4},children:[(0,i.jsx)(d.Tj,{children:"Timestamp"}),(0,i.jsx)("div",{children:t?(0,i.jsx)(S.E,{timestamp:{unix:t.timestamp},timeFormat:{showTimezone:!0}}):"–"})]}),(0,i.jsxs)(a.x,{flex:{direction:"column",gap:4},children:[(0,i.jsx)(d.Tj,{children:"Duration"}),(0,i.jsx)("div",{children:t?.endTimestamp?(0,R.R4)(1e3*t.endTimestamp-1e3*t.timestamp):"–"})]})]})})}function $(e){let{partitions:t}=e;return(0,i.jsxs)(x.i,{children:[(0,i.jsx)("thead",{children:(0,i.jsxs)("tr",{children:[(0,i.jsx)("th",{children:"Partition definition"}),(0,i.jsx)("th",{children:"Partition"})]})}),(0,i.jsx)("tbody",{children:t.flatMap(e=>e.partitionKeys?.map(t=>i.jsxs("tr",{children:[i.jsx("td",{children:i.jsx(h.g,{text:e.partitionsDefName})}),i.jsx("td",{children:i.jsx(h.g,{text:t})})]},t)))})]})}let P=(0,f.Ps)`
  query SelectedTickQuery($instigationSelector: InstigationSelector!, $tickId: ID!) {
    instigationStateOrError(instigationSelector: $instigationSelector) {
      ... on InstigationState {
        id
        tick(tickId: $tickId) {
          id
          ...HistoryTick

          requestedAssetKeys {
            path
          }
          requestedAssetMaterializationCount
          autoMaterializeAssetEvaluationId
          requestedMaterializationsForAssets {
            assetKey {
              path
            }
            partitionKeys
          }
        }
      }
    }
  }

  ${v.B}
  ${j.vw}
`},5620:function(e,t,n){n.d(t,{u:function(){return O}});var i=n(52322),s=n(14934),r=n(35292),a=n(39013),o=n(18259),l=n(74159),d=n(36853),c=n(79106),u=n(89891),x=n(88510),h=n(2784),p=n(47933),m=n(2728),j=n(20958),g=n(44900),f=n(84842),y=n(37680),v=n(92506),I=n(45774),R=n(37483),S=n(38491),b=n(58453),T=n(78367),w=n(5034);let E=e=>{let{evaluation:t,assetKeyPath:n,selectedPartition:s,setSelectedPartition:r}=e,{data:a}=(0,j.aM)(b.s,{variables:{assetKey:{path:n},evaluationId:t.evaluationId,partition:s},skip:!s});return(0,i.jsx)(w.M,{assetKeyPath:n,evaluationId:t.evaluationId,evaluationNodes:t.isLegacy?s&&a?.assetConditionEvaluationForPartition?a.assetConditionEvaluationForPartition.evaluationNodes:t.evaluation.evaluationNodes:t.evaluationNodes,isLegacyEvaluation:t.isLegacy,rootUniqueId:t.evaluation.rootUniqueId,selectPartition:r})};var k=n(86664),$=n(96702),P=n(86269);let F=e=>{let{isOpen:t,setIsOpen:n,evaluationID:s,assetKeyPath:r}=e;return(0,i.jsx)(v.Vq,{isOpen:t,onClose:()=>n(!1),style:A,children:(0,i.jsx)(K,{evaluationID:s,assetKeyPath:r,setIsOpen:n})})},K=e=>{let{evaluationID:t,assetKeyPath:n,setIsOpen:a}=e,[o,l]=(0,h.useState)(null),{data:d,loading:c}=(0,j.aM)(b.G,{variables:{assetKey:{path:n},cursor:`${BigInt(t)+1n}`,limit:2}}),{partitions:u,loading:x}=(0,k.D)(n);if(c||x)return(0,i.jsx)(C,{header:(0,i.jsx)(v.fK,{icon:"automation",label:"Evaluation details"}),body:(0,i.jsx)(s.x,{padding:{top:64},flex:{direction:"row",justifyContent:"center"},children:(0,i.jsx)(I.f,{label:"Loading evaluation details..."})}),onDone:()=>a(!1)});let p=d?.assetConditionEvaluationRecordsOrError;if(p?.__typename==="AutoMaterializeAssetEvaluationNeedsMigrationError")return(0,i.jsx)(C,{header:(0,i.jsx)(v.fK,{icon:"automation",label:"Evaluation details"}),body:(0,i.jsx)(s.x,{margin:{top:64},children:(0,i.jsx)(R.t,{icon:"automation",title:"Evaluation needs migration",description:p.message})}),onDone:()=>a(!1)});let m=p?.records[0];return m?(0,i.jsx)(C,{header:(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(v.fK,{icon:"automation",label:(0,i.jsxs)("div",{children:["Evaluation details:"," ",(0,i.jsx)(P.v,{timestamp:m.timestamp,timeFormat:{...$._,showSeconds:!0}})]})}),u.length>0?(0,i.jsx)(s.x,{padding:{vertical:12,right:20},flex:{justifyContent:"flex-end"},children:(0,i.jsx)(T.w,{allPartitions:u,selectedPartition:o,selectPartition:l})}):null]}),body:(0,i.jsx)(E,{evaluation:m,assetKeyPath:n,selectedPartition:o,setSelectedPartition:l}),onDone:()=>a(!1)}):(0,i.jsx)(C,{header:(0,i.jsx)(v.fK,{icon:"automation",label:"Evaluation details"}),body:(0,i.jsx)(s.x,{margin:{top:64},children:(0,i.jsx)(R.t,{icon:"automation",title:"Evaluation not found",description:(0,i.jsxs)(i.Fragment,{children:["Evaluation ",(0,i.jsx)(r.fv,{children:t})," not found"]})})}),onDone:()=>a(!1)})},C=e=>{let{header:t,body:n,onDone:r}=e;return(0,i.jsxs)(s.x,{flex:{direction:"column"},style:{height:"100%"},children:[t,(0,i.jsx)("div",{style:{flex:1,overflowY:"auto"},children:n}),(0,i.jsx)("div",{style:{flexGrow:0},children:(0,i.jsx)(v.cN,{topBorder:!0,children:(0,i.jsx)(S.zx,{onClick:r,children:"Done"})})})]})},A={width:"80vw",maxWidth:"1400px",minWidth:"800px",height:"80vh",minHeight:"400px",maxHeight:"1400px"};var q=n(17008),z=n(65822),D=n(10539);let N="30% 17% 53%",O=e=>{let{tick:t}=e,[n,d]=(0,h.useState)(""),c=(0,h.useMemo)(()=>t?t.requestedAssetKeys.filter(e=>e.path.join("/").includes(n)):[],[t,n]),u=(0,h.useRef)(null),p=(0,x.MG)({count:c.length,getScrollElement:()=>u.current,estimateSize:()=>34,overscan:10}),m=p.getTotalSize(),j=p.getVirtualItems(),f=(0,h.useMemo)(()=>{let e={};return t?.requestedMaterializationsForAssets.forEach(t=>{let{assetKey:n,partitionKeys:i}=t;e[g.Je(n)]=i}),e},[t?.requestedMaterializationsForAssets]);return(0,i.jsxs)(s.x,{style:{height:"500px"},flex:{direction:"column"},children:[(0,i.jsxs)(s.x,{padding:{vertical:12,horizontal:24},flex:{justifyContent:"space-between",alignItems:"center"},border:"bottom",children:[(0,i.jsx)(r.Tj,{children:"Requested materializations"}),(0,i.jsx)(l.oi,{icon:"search",value:n,onChange:e=>d(e.target.value),placeholder:"Filter by asset key…",style:{width:"252px"}})]}),n&&!c.length?(0,i.jsx)(y.LS,{title:"No matching asset keys",description:(0,i.jsxs)(i.Fragment,{children:["No matching asset keys for ",(0,i.jsx)("strong",{children:n})]})}):t?.requestedAssetKeys.length?(0,i.jsxs)(q.W2,{ref:u,children:[(0,i.jsxs)(q.VJ,{templateColumns:N,sticky:!0,children:[(0,i.jsx)(o.qN,{children:"Asset"}),(0,i.jsx)(o.qN,{children:"Group"}),(0,i.jsx)(o.qN,{children:"Result"})]}),(0,i.jsx)(o.Nh,{$totalHeight:m,children:j.map(e=>{let{index:n,key:s,size:r,start:a}=e,o=c[n];return(0,i.jsx)(M,{$height:r,$start:a,assetKey:o,partitionKeys:f[(0,g.Je)(o)],evaluationId:t.autoMaterializeAssetEvaluationId},s)})})]}):(0,i.jsx)(s.x,{padding:{vertical:12,horizontal:24},children:(0,i.jsx)(r.YS,{color:a.$(),children:"None"})})]})},M=e=>{let{$start:t,$height:n,assetKey:l,partitionKeys:x,evaluationId:m}=e,[g,y]=(0,h.useState)(!1),v=x?.length||1,{data:I}=(0,j.aM)(L,{fetchPolicy:"cache-and-network",variables:{assetKey:{path:l.path}}}),R=I?.assetOrError.__typename==="Asset"?I.assetOrError:null,S=R?.definition,b=S?(0,z.kQ)(S.repository.name,S.repository.location.name):null;return(0,i.jsx)(o.X2,{$start:t,$height:n,children:(0,i.jsxs)(_,{border:"bottom",children:[(0,i.jsx)(o.E5,{children:(0,i.jsx)(f.R,{path:l.path,icon:"asset",textStyle:"middle-truncate"})}),(0,i.jsx)(o.E5,{children:I?S&&S.groupName&&b?(0,i.jsx)(p.rU,{to:(0,D.$U)(b,`/asset-groups/${S.groupName}`),children:(0,i.jsxs)(s.x,{flex:{direction:"row",gap:8,alignItems:"center"},children:[(0,i.jsx)(d.JO,{color:a.$(),name:"asset_group"}),S.groupName]})}):(0,i.jsx)(r.YS,{color:a.$(),children:"Asset not found"}):(0,i.jsx)(c.$,{purpose:"body-text"})}),(0,i.jsx)(o.E5,{children:S?(0,i.jsxs)(i.Fragment,{children:[(0,i.jsxs)(u.Z,{onClick:()=>y(!0),children:[v," materialization",1===v?"":"s"," ","requested"]}),(0,i.jsx)(F,{isOpen:g,setIsOpen:y,evaluationID:m,assetKeyPath:l.path})]}):null})]})})},_=(0,m.ZP)(s.x).withConfig({componentId:"sc-42767682-0"})(["display:grid;grid-template-columns:",";height:100%;> *{justify-content:center;}"],N),L=(0,j.Ps)`
  query AssetGroupAndLocationQuery($assetKey: AssetKeyInput!) {
    assetOrError(assetKey: $assetKey) {
      ... on Asset {
        id
        definition {
          id
          groupName
          repository {
            id
            name
            location {
              id
              name
            }
          }
        }
      }
    }
  }
`},84046:function(e,t,n){n.d(t,{JH:function(){return g},qL:function(){return f}});var i=n(52322),s=n(14934),r=n(55010),a=n(35292),o=n(39013),l=n(74188),d=n(47933),c=n(2728),u=n(54516),x=n(67994),h=n(59351),p=n(33182),m=n(90797);let j=e=>{let{status:t,opacity:n=1}=e,s=x.YM[t];return(0,i.jsx)(S,{$color:s,$opacity:n})},g=e=>{let{fade:t,jobName:n,runs:a}=e,o=a.length,l=.8/Math.max(3,o);return(0,i.jsx)(s.x,{flex:{direction:"row",alignItems:"center",gap:2},children:a.map((e,s)=>(0,i.jsx)(r.J,{position:"top",interactionKind:"hover",content:(0,i.jsx)("div",{children:(0,i.jsx)(f,{run:e,name:n})}),hoverOpenDelay:100,children:(0,i.jsx)(j,{runId:e.id,status:e.status,opacity:t?1-(o-s-1)*l:1},e.id)},e.id))})},f=e=>{let{name:t,run:n}=e;return(0,i.jsxs)(y,{children:[(0,i.jsx)(v,{children:t}),(0,i.jsxs)(I,{children:[(0,i.jsxs)(s.x,{flex:{alignItems:"center",direction:"row",gap:8},children:[(0,i.jsx)(u.K9,{status:n.status}),(0,i.jsx)(d.rU,{to:`/runs/${n.id}`,children:(0,i.jsx)(a.aU,{children:(0,p.EI)(n)})})]}),(0,i.jsxs)(s.x,{flex:{direction:"column",gap:4},children:[(0,i.jsx)(p.Zr,{run:n}),(0,i.jsx)(p.Tw,{run:n})]})]}),h.hd.has(n.status)||h.Ys.has(n.status)?(0,i.jsx)(R,{children:(0,i.jsx)(m.I,{runId:n.id})}):null]})},y=c.ZP.div.withConfig({componentId:"sc-d6498aea-0"})(["padding:4px;font-size:12px;width:220px;"]),v=c.ZP.div.withConfig({componentId:"sc-d6498aea-1"})(["padding:8px;box-shadow:inset 0 -1px ",";font-family:",";font-size:14px;font-weight:500;color:",";max-width:100%;text-overflow:ellipsis;overflow:hidden;min-width:0px;"],o.wL(),l.b.default,o.Ep()),I=c.ZP.div.withConfig({componentId:"sc-d6498aea-2"})(["padding:8px;display:flex;align-items:flex-start;justify-content:space-between;"]),R=c.ZP.div.withConfig({componentId:"sc-d6498aea-3"})(["overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:4px 8px 8px;&:empty{display:none;}"]),S=c.ZP.div.withConfig({componentId:"sc-d6498aea-4"})(["background-color:",";border-radius:2px;height:16px;opacity:",";width:8px;"],e=>{let{$color:t}=e;return t},e=>{let{$opacity:t}=e;return t})}}]);
//# sourceMappingURL=4030.7c49a3fb74d25048.js.map