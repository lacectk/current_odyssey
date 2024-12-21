"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[6023],{6023:function(e,n,t){t.r(n),t.d(n,{ConcurrencyLimits:function(){return en},FREE_CONCURRENCY_SLOTS_MUTATION:function(){return ej},INSTANCE_CONCURRENCY_LIMITS_QUERY:function(){return eh},InstanceConcurrencyPage:function(){return H},InstanceConcurrencyPageContent:function(){return W},RunConcurrencyContent:function(){return X},default:function(){return G}});var r=t(52322),i=t(14934),s=t(79106),c=t(49308),o=t(74580),l=t(35292),a=t(90441),d=t(38491),u=t(36853),x=t(37483),y=t(89891),h=t(43212),m=t(74159),p=t(92506),j=t(55010),g=t(60603),f=t(35002),C=t(39013),b=t(1747),v=t(20156),S=t(2784),I=t(47933),K=t(9108),w=t(77265),T=t(58495),k=t(88510),q=t(2728),N=t(20958),R=t(16336),$=t(17008),E=t(83738);let F="1fr 150px 150px 150px 150px 150px",L=e=>{let{concurrencyKeys:n,onEdit:t,onDelete:i,onSelect:s}=e,c=(0,S.useRef)(null),o=(0,k.MG)({count:n.length,getScrollElement:()=>c.current,estimateSize:()=>64,overscan:10}),l=o.getTotalSize(),a=o.getVirtualItems();return(0,r.jsx)("div",{style:{overflow:"hidden"},children:(0,r.jsxs)($.W2,{ref:c,children:[(0,r.jsx)(_,{}),(0,r.jsx)($.Nh,{$totalHeight:l,children:a.map(e=>{let{index:c,key:o,size:l,start:a}=e,d=n[c];return(0,r.jsx)(z,{concurrencyKey:d,onEdit:t,onDelete:i,onSelect:s,height:l,start:a},o)})})]})})},_=()=>(0,r.jsxs)($.VJ,{templateColumns:F,sticky:!0,children:[(0,r.jsx)($.qN,{children:"Concurrency key"}),(0,r.jsx)($.qN,{children:"Total slots"}),(0,r.jsx)($.qN,{children:"Assigned steps"}),(0,r.jsx)($.qN,{children:"Pending steps"}),(0,r.jsx)($.qN,{children:"All steps"}),(0,r.jsx)($.qN,{})]}),z=e=>{let{concurrencyKey:n,onEdit:t,onDelete:s,onSelect:c,height:o,start:l}=e,a=(0,T.F)(100),d=(0,N.aM)(Q,{variables:{concurrencyKey:n},skip:!a});(0,R.C4)(d,R.dT);let{data:u}=d,x=u?.instance.concurrencyLimit;return(0,r.jsx)($.X2,{$height:o,$start:l,children:(0,r.jsxs)(P,{border:"bottom",children:[(0,r.jsx)($.E5,{children:(0,r.jsx)(r.Fragment,{children:n})}),(0,r.jsx)($.E5,{children:x?(0,r.jsx)("div",{children:x.slotCount}):(0,r.jsx)(E.mq,{queryResult:d})}),(0,r.jsx)($.E5,{children:x?(0,r.jsx)(r.Fragment,{children:x.pendingSteps.filter(e=>!!e.assignedTimestamp).length}):(0,r.jsx)(E.mq,{queryResult:d})}),(0,r.jsx)($.E5,{children:x?(0,r.jsx)(r.Fragment,{children:x.pendingSteps.filter(e=>!e.assignedTimestamp).length}):(0,r.jsx)(E.mq,{queryResult:d})}),(0,r.jsx)($.E5,{children:x?(0,r.jsxs)(i.x,{flex:{direction:"row",gap:16,alignItems:"center"},children:[(0,r.jsx)("span",{children:x.pendingSteps.length}),(0,r.jsx)(h.V,{intent:"primary",interactive:!0,children:(0,r.jsx)(y.Z,{onClick:()=>{c(x.concurrencyKey)},children:"View all"})})]}):(0,r.jsx)(E.mq,{queryResult:d})}),(0,r.jsx)($.E5,{children:(0,r.jsx)(O,{concurrencyKey:n,onEdit:t,onDelete:s})})]})})},O=e=>{let{concurrencyKey:n,onDelete:t,onEdit:i}=e;return(0,r.jsx)(j.J,{content:(0,r.jsxs)(g.v2,{children:[(0,r.jsx)(g.sN,{icon:"edit",text:"Edit",onClick:()=>i(n)}),(0,r.jsx)(g.sN,{icon:"delete",intent:"danger",text:"Delete",onClick:()=>t(n)})]}),position:"bottom-left",children:(0,r.jsx)(d.zx,{icon:(0,r.jsx)(u.JO,{name:"expand_more"})})})},Q=(0,N.Ps)`
  query SingleConcurrencyKeyQuery($concurrencyKey: String!) {
    instance {
      id
      concurrencyLimit(concurrencyKey: $concurrencyKey) {
        concurrencyKey
        slotCount
        claimedSlots {
          runId
          stepKey
        }
        pendingSteps {
          runId
          stepKey
          enqueuedTimestamp
          assignedTimestamp
          priority
        }
      }
    }
  }
`,P=(0,q.ZP)(i.x).withConfig({componentId:"sc-77537c9e-0"})(["display:grid;grid-template-columns:",";height:100%;"],F);var V=t(50309),D=t(71406),U=t(69737),A=t(20171),J=t(4378),M=t(54516),Y=t(59351),B=t(33182),Z=t(2278);let W=S.memo(()=>{(0,U.Px)(),(0,A.j)("Concurrency");let[e,n]=(0,J.q)({queryKey:"key"}),t=(0,N.aM)(eh,{notifyOnNetworkStatusChange:!0}),c=(0,R.C4)(t,R.dT),{data:o}=t;return(0,r.jsx)("div",{style:{overflowY:"auto"},children:o?(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(i.x,{margin:{bottom:64},children:(0,r.jsx)(X,{refreshState:c,hasRunQueue:!!o?.instance.runQueuingSupported,runQueueConfig:o?.instance.runQueueConfig})}),(0,r.jsx)(en,{instanceConfig:o.instance.info,concurrencyKeys:o.instance.concurrencyLimits.map(e=>e.concurrencyKey),hasSupport:o.instance.supportsConcurrencyLimits,refetch:t.refetch,minValue:o.instance.minConcurrencyLimitValue,maxValue:o.instance.maxConcurrencyLimitValue,selectedKey:e,onSelectKey:n})]}):(0,r.jsx)(i.x,{padding:{vertical:64},children:(0,r.jsx)(s.$,{purpose:"section"})})})}),H=()=>{let{pageTitle:e}=S.useContext(K.N);return(0,r.jsxs)(c.T,{style:{padding:0},children:[(0,r.jsx)(o.m,{title:(0,r.jsx)(l.X6,{children:e}),tabs:(0,r.jsx)(w.Z,{tab:"concurrency"})}),(0,r.jsx)(W,{})]})};var G=H;let X=e=>{let{hasRunQueue:n,runQueueConfig:t,onEdit:s,refreshState:c}=e;if(!n)return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsxs)(i.x,{padding:{vertical:16,horizontal:24},border:"bottom",flex:{direction:"row",alignItems:"center",justifyContent:"space-between"},children:[(0,r.jsx)(l.pm,{children:"Run concurrency"}),c?(0,r.jsx)(R.xi,{refreshState:c}):null]}),(0,r.jsxs)(i.x,{padding:{vertical:16,horizontal:24},children:["Run concurrency is not supported with this run coordinator. To enable run concurrency limits, configure your instance to use the ",(0,r.jsx)(l.fv,{children:"QueuedRunCoordinator"})," in your"," ",(0,r.jsx)(l.fv,{children:"dagster.yaml"}),". See the"," ",(0,r.jsx)("a",{target:"_blank",rel:"noreferrer",href:"https://docs.dagster.io/deployment/dagster-instance#queuedruncoordinator",children:"QueuedRunCoordinator documentation"})," ","for more information."]})]});let o=(0,r.jsxs)(i.x,{padding:{vertical:16,horizontal:24},children:["Run concurrency can be set in your run queue settings. See the"," ",(0,r.jsx)("a",{target:"_blank",rel:"noreferrer",href:"https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#configuring-run-level-concurrency",children:"run concurrency documentation"})," ","for more information."]}),d=t?(0,r.jsx)(a.fJ,{children:(0,r.jsxs)("tbody",{children:[(0,r.jsxs)("tr",{children:[(0,r.jsx)("td",{children:"Max concurrent runs:"}),(0,r.jsx)("td",{children:t.maxConcurrentRuns})]}),(0,r.jsxs)("tr",{children:[(0,r.jsx)("td",{children:"Tag concurrency limits:"}),(0,r.jsx)("td",{children:t.tagConcurrencyLimitsYaml?(0,r.jsx)(v.u,{value:t.tagConcurrencyLimitsYaml,options:{readOnly:!0,lineNumbers:!0,mode:"yaml"}}):"-"})]})]})}):null;return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(ee,{onEdit:s,refreshState:c}),o,d]})},ee=e=>{let{onEdit:n,refreshState:t}=e;return(0,r.jsxs)(i.x,{flex:{justifyContent:"space-between",alignItems:"center"},padding:{vertical:16,horizontal:24},border:"bottom",children:[(0,r.jsx)(l.pm,{children:"Run concurrency"}),(0,r.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[t?(0,r.jsx)(R.xi,{refreshState:t}):null,n?(0,r.jsx)(d.zx,{icon:(0,r.jsx)(u.JO,{name:"edit"}),onClick:()=>n(),children:"Edit configuration"}):null]})]})},en=e=>{let{instanceConfig:n,hasSupport:t,concurrencyKeys:s,refetch:c,minValue:o,maxValue:l,selectedKey:a,onSelectKey:d}=e,[u,h]=S.useState(),m=S.useCallback(()=>{d(void 0)},[d]),[p,j]=S.useState(""),g=S.useMemo(()=>[...s].filter(e=>e.includes(p)).sort((e,n)=>D.Bl.compare(e,n)),[s,p]),f=()=>{h({actionType:"add"})};return!t&&n&&n.includes("SqliteEventLogStorage")?(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(et,{}),(0,r.jsx)(i.x,{margin:24,children:(0,r.jsx)(x.t,{icon:"error",title:"No concurrency support",description:"This instance does not support global concurrency limits. You will need to configure a different storage implementation (e.g. Postgres/MySQL) to use this feature."})})]}):!1===t?(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(et,{}),(0,r.jsx)(i.x,{margin:24,children:(0,r.jsx)(x.t,{icon:"error",title:"No concurrency support",description:"This instance does not currently support global concurrency limits. You may need to run `dagster instance migrate` to add the necessary tables to your dagster storage to support this feature."})})]}):(0,r.jsxs)(i.x,{flex:{direction:"column"},style:{overflow:"auto",height:"100%"},children:[(0,r.jsx)(et,{onAdd:f,search:p,setSearch:j}),0===s.length?(0,r.jsx)(i.x,{margin:24,children:(0,r.jsx)(x.t,{icon:"error",title:"No concurrency limits",description:(0,r.jsxs)(r.Fragment,{children:["No concurrency limits have been configured for this instance.\xa0",(0,r.jsx)(y.Z,{onClick:()=>f(),children:"Add a concurrency limit"}),"."]})})}):(0,r.jsx)(L,{concurrencyKeys:g,onDelete:e=>{h({actionType:"delete",concurrencyKey:e})},onEdit:e=>{h({actionType:"edit",concurrencyKey:e})},onSelect:d}),(0,r.jsx)(ei,{open:u?.actionType==="add",onClose:()=>h(void 0),onComplete:c,minValue:o??1,maxValue:l??1e3}),(0,r.jsx)(ec,{concurrencyKey:u&&"delete"===u.actionType?u.concurrencyKey:"",open:u?.actionType==="delete",onClose:()=>h(void 0),onComplete:c}),(0,r.jsx)(es,{open:u?.actionType==="edit",onClose:()=>h(void 0),onComplete:c,concurrencyKey:u?.actionType==="edit"?u.concurrencyKey:"",minValue:o??1,maxValue:l??1e3}),(0,r.jsx)(el,{title:(0,r.jsxs)("span",{children:["Concurrency steps for ",(0,r.jsx)("strong",{children:a})]}),onClose:m,concurrencyKey:a,onUpdate:c})]})},et=e=>{let{onAdd:n,setSearch:t,search:s}=e;return(0,r.jsxs)(i.x,{flex:{direction:"column"},children:[(0,r.jsxs)(i.x,{flex:{justifyContent:"space-between",alignItems:"center"},padding:{vertical:16,horizontal:24},border:"top-and-bottom",children:[(0,r.jsxs)(i.x,{flex:{alignItems:"center",direction:"row",gap:8},children:[(0,r.jsx)(l.pm,{children:"Global op/asset concurrency"}),(0,r.jsx)(h.V,{children:"Experimental"})]}),n?(0,r.jsx)(d.zx,{icon:(0,r.jsx)(u.JO,{name:"add_circle"}),onClick:()=>n(),children:"Add concurrency limit"}):null]}),t?(0,r.jsx)(i.x,{flex:{direction:"row"},padding:{vertical:16,horizontal:24},border:"bottom",children:(0,r.jsx)(m.oi,{value:s||"",style:{width:"30vw",minWidth:150,maxWidth:400},placeholder:"Filter concurrency keys",onChange:e=>t(e.target.value)})}):null]})},er=function(e){let n=arguments.length>1&&void 0!==arguments[1]?arguments[1]:1,t=arguments.length>2&&void 0!==arguments[2]?arguments[2]:1e3;if(!e)return!1;let r=parseInt(e);return!isNaN(r)&&String(r)===e.trim()&&r>=n&&r<=t},ei=e=>{let{open:n,onClose:t,onComplete:s,maxValue:c,minValue:o}=e,[l,a]=S.useState(!1),[u,x]=S.useState(""),[y,h]=S.useState("");S.useEffect(()=>{x(""),h("")},[n]);let[j]=(0,N.Db)(em),g=async()=>{a(!0),await j({variables:{concurrencyKey:y,limit:parseInt(u.trim())}}),a(!1),s(),t()};return(0,r.jsxs)(p.Vq,{isOpen:n,title:"Add concurrency limit",onClose:t,children:[(0,r.jsxs)(p.a7,{children:[(0,r.jsx)(i.x,{margin:{bottom:4},children:"Concurrency key:"}),(0,r.jsx)(i.x,{margin:{bottom:16},children:(0,r.jsx)(m.oi,{value:y||"",onChange:e=>h(e.target.value),placeholder:"Concurrency key"})}),(0,r.jsxs)(i.x,{margin:{bottom:4},children:["Concurrency limit (",o,"-",c,"):"]}),(0,r.jsx)(i.x,{children:(0,r.jsx)(m.oi,{value:u||"",onChange:e=>x(e.target.value),placeholder:`${o} - ${c}`})})]}),(0,r.jsxs)(p.cN,{children:[(0,r.jsx)(d.zx,{intent:"none",onClick:t,children:"Cancel"}),(0,r.jsx)(d.zx,{intent:"primary",onClick:g,disabled:!er(u.trim(),o,c)||!y||l,children:l?"Adding...":"Add limit"})]})]})},es=e=>{let{concurrencyKey:n,open:t,onClose:s,onComplete:c,minValue:o,maxValue:a}=e,[u,x]=S.useState(!1),[y,h]=S.useState("");S.useEffect(()=>{h("")},[t]);let[j]=(0,N.Db)(em),g=async()=>{x(!0),await j({variables:{concurrencyKey:n,limit:parseInt(y.trim())}}),x(!1),c(),s()},f=(0,r.jsxs)(r.Fragment,{children:["Edit ",(0,r.jsx)(l.fv,{children:n})]});return(0,r.jsxs)(p.Vq,{isOpen:t,title:f,onClose:s,children:[(0,r.jsxs)(p.a7,{children:[(0,r.jsx)(i.x,{margin:{bottom:4},children:"Concurrency key:"}),(0,r.jsx)(i.x,{margin:{bottom:16},children:(0,r.jsx)("strong",{children:n})}),(0,r.jsxs)(i.x,{margin:{bottom:4},children:["Concurrency limit (",o,"-",a,"):"]}),(0,r.jsx)(i.x,{children:(0,r.jsx)(m.oi,{value:y||"",onChange:e=>h(e.target.value),placeholder:`${o} - ${a}`})})]}),(0,r.jsxs)(p.cN,{children:[(0,r.jsx)(d.zx,{intent:"none",onClick:s,children:"Close"}),u?(0,r.jsx)(d.zx,{intent:"primary",disabled:!0,children:"Updating..."}):(0,r.jsx)(d.zx,{intent:"primary",onClick:g,disabled:!er(y.trim(),o,a),children:"Update limit"})]})]})},ec=e=>{let{concurrencyKey:n,open:t,onClose:i,onComplete:s}=e,[c,o]=S.useState(!1),[a]=(0,N.Db)(ep),u=async()=>{o(!0),await a({variables:{concurrencyKey:n}}),o(!1),s(),i()},x=(0,r.jsxs)(r.Fragment,{children:["Delete ",(0,r.jsx)(l.fv,{children:n})]});return(0,r.jsxs)(p.Vq,{isOpen:t,title:x,onClose:i,children:[(0,r.jsxs)(p.a7,{children:["Delete concurrency limit\xa0",(0,r.jsx)("strong",{children:n}),"?"]}),(0,r.jsxs)(p.cN,{children:[(0,r.jsx)(d.zx,{intent:"none",onClick:i,children:"Close"}),c?(0,r.jsx)(d.zx,{intent:"danger",disabled:!0,children:"Deleting..."}):(0,r.jsx)(d.zx,{intent:"danger",onClick:u,children:"Delete limit"})]})]})},eo=e=>{let{pendingStep:n,onUpdate:t}=e,[i]=(0,N.Db)(ej);return(0,r.jsx)(j.J,{content:(0,r.jsxs)(g.v2,{children:[(0,r.jsx)(g.sN,{icon:"status",text:"Free concurrency slot for step",onClick:async()=>{let e=await i({variables:{runId:n.runId,stepKey:n.stepKey}});e.data?.freeConcurrencySlots&&(t(),await (0,V.B7)({intent:"success",icon:"copy_to_clipboard_done",message:"Freed concurrency slot"}))}},"free-concurrency-slots-step"),(0,r.jsx)(g.sN,{icon:"status",text:"Free all concurrency slots for run",onClick:async()=>{await (0,V.B7)({message:"Freeing concurrency slots..."});let e=await i({variables:{runId:n.runId}});e.data?.freeConcurrencySlots&&(t(),await (0,V.B7)({intent:"success",icon:"copy_to_clipboard_done",message:"Freed concurrency slots"}))}},"free-concurrency-slots-run")]}),position:"bottom-right",children:(0,r.jsx)(d.zx,{rightIcon:(0,r.jsx)(u.JO,{name:"expand_more"}),children:"Actions"})})},el=e=>{let{concurrencyKey:n,onClose:t,title:c,onUpdate:o}=e,l=(0,N.aM)(eg,{variables:{concurrencyKey:n||""},skip:!n});(0,R.C4)(l,R.dT);let{data:a}=l,u=S.useCallback(()=>{l.refetch(),o()},[l,o]);return(0,r.jsxs)(p.Vq,{isOpen:!!n,title:c,onClose:t,style:{minWidth:"400px",maxWidth:"1000px",width:"90vw",maxHeight:"90vh"},children:[(0,r.jsx)(i.x,{padding:{vertical:16},flex:{grow:1},style:{overflowY:"auto"},children:a?(0,r.jsx)(ea,{keyInfo:a.instance.concurrencyLimit,refresh:u}):(0,r.jsx)(i.x,{padding:{vertical:64},children:(0,r.jsx)(s.$,{purpose:"section"})})}),(0,r.jsx)(p.cN,{children:(0,r.jsx)(d.zx,{intent:"none",onClick:t,children:"Close"})})]})},ea=e=>{let{keyInfo:n,refresh:t}=e,s=[...new Set(n.pendingSteps.map(e=>e.runId))],c=(0,N.aM)(ef,{variables:{filter:{runIds:s}},skip:!n.pendingSteps.length}),o={};(c.data?.pipelineRunsOrError.__typename==="Runs"?c.data.pipelineRunsOrError.results:[]).forEach(e=>{o[e.id]=e.status});let l=[...n.pendingSteps];l.sort((e,n)=>e.priority&&n.priority&&e.priority!==n.priority?e.priority-n.priority:e.enqueuedTimestamp-n.enqueuedTimestamp);let a=l.filter(e=>!!e.assignedTimestamp),d=l.filter(e=>!e.assignedTimestamp),x=(0,r.jsx)("thead",{children:(0,r.jsxs)("tr",{children:[(0,r.jsx)("th",{children:"Run ID"}),(0,r.jsx)("th",{children:"Step key"}),(0,r.jsx)("th",{children:"Assigned"}),(0,r.jsx)("th",{children:"Queued"}),(0,r.jsx)("th",{children:(0,r.jsxs)(i.x,{flex:{alignItems:"center",direction:"row",gap:4},children:["Priority",(0,r.jsx)(f.u,{placement:"top",content:"Priority can be set on each op/asset using the 'dagster/priority' tag. Higher priority steps will be assigned slots first.",children:(0,r.jsx)(u.JO,{name:"info",color:C.md()})})]})}),(0,r.jsx)("th",{})]})});return l.length?(0,r.jsxs)(b.i,{children:[x,(0,r.jsx)("tbody",{style:{backgroundColor:C.B5()},children:a.map(e=>(0,r.jsx)(ed,{step:e,statusByRunId:o,onUpdate:t},e.runId+e.stepKey))}),(0,r.jsx)("tbody",{children:d.map(e=>(0,r.jsx)(ed,{step:e,statusByRunId:o,onUpdate:t},e.runId+e.stepKey))})]}):(0,r.jsxs)(b.i,{children:[x,(0,r.jsx)("tbody",{children:(0,r.jsx)("tr",{children:(0,r.jsx)("td",{colSpan:6,children:(0,r.jsx)(i.x,{flex:{alignItems:"center",justifyContent:"center"},style:{color:C.$()},padding:16,children:"There are no active or pending steps for this concurrency key."})})})})]})},ed=e=>{let{step:n,statusByRunId:t,onUpdate:s}=e,c=t[n.runId];return(0,r.jsxs)("tr",{children:[(0,r.jsx)("td",{children:c?(0,r.jsx)(I.rU,{to:`/runs/${n.runId}`,children:(0,r.jsxs)(i.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[(0,r.jsx)(M.O_,{status:c,size:10}),(0,r.jsx)(l.fv,{children:(0,B.EI)({id:n.runId})}),Y.hd.has(c)?(0,r.jsx)(f.u,{placement:"top",content:"Slots for canceled / failed runs can automatically be freed by configuring a run monitoring setting.",children:(0,r.jsx)(u.JO,{name:"info",color:C.md()})}):null]})}):(0,r.jsx)(l.fv,{children:(0,B.EI)({id:n.runId})})}),(0,r.jsx)("td",{children:(0,r.jsx)(l.fv,{children:n.stepKey})}),(0,r.jsx)("td",{children:n.assignedTimestamp?(0,r.jsx)(Z.Q,{startUnix:n.assignedTimestamp,endUnix:null}):"-"}),(0,r.jsx)("td",{children:n.enqueuedTimestamp?(0,r.jsx)(Z.Q,{startUnix:n.enqueuedTimestamp,endUnix:null}):"-"}),(0,r.jsx)("td",{children:n.priority}),(0,r.jsx)("td",{children:(0,r.jsx)(eo,{pendingStep:n,onUpdate:s})})]})},eu=(0,N.Ps)`
  fragment ConcurrencyStepFragment on PendingConcurrencyStep {
    runId
    stepKey
    enqueuedTimestamp
    assignedTimestamp
    priority
  }
`,ex=(0,N.Ps)`
  fragment ConcurrencyLimitFragment on ConcurrencyKeyInfo {
    concurrencyKey
    slotCount
    claimedSlots {
      runId
      stepKey
    }
    pendingSteps {
      ...ConcurrencyStepFragment
    }
  }
  ${eu}
`,ey=(0,N.Ps)`
  fragment RunQueueConfigFragment on RunQueueConfig {
    maxConcurrentRuns
    tagConcurrencyLimitsYaml
  }
`,eh=(0,N.Ps)`
  query InstanceConcurrencyLimitsQuery {
    instance {
      id
      info
      supportsConcurrencyLimits
      runQueuingSupported
      runQueueConfig {
        ...RunQueueConfigFragment
      }
      minConcurrencyLimitValue
      maxConcurrencyLimitValue
      concurrencyLimits {
        concurrencyKey
      }
    }
  }

  ${ey}
`,em=(0,N.Ps)`
  mutation SetConcurrencyLimit($concurrencyKey: String!, $limit: Int!) {
    setConcurrencyLimit(concurrencyKey: $concurrencyKey, limit: $limit)
  }
`,ep=(0,N.Ps)`
  mutation DeleteConcurrencyLimit($concurrencyKey: String!) {
    deleteConcurrencyLimit(concurrencyKey: $concurrencyKey)
  }
`,ej=(0,N.Ps)`
  mutation FreeConcurrencySlots($runId: String!, $stepKey: String) {
    freeConcurrencySlots(runId: $runId, stepKey: $stepKey)
  }
`,eg=(0,N.Ps)`
  query ConcurrencyKeyDetailsQuery($concurrencyKey: String!) {
    instance {
      id
      concurrencyLimit(concurrencyKey: $concurrencyKey) {
        ...ConcurrencyLimitFragment
      }
    }
  }
  ${ex}
`,ef=(0,N.Ps)`
  query RunsForConcurrencyKeyQuery($filter: RunsFilter, $limit: Int) {
    pipelineRunsOrError(filter: $filter, limit: $limit) {
      ... on Runs {
        results {
          id
          status
        }
      }
    }
  }
`},77265:function(e,n,t){t.d(n,{Z:function(){return f}});var r=t(52322),i=t(14934),s=t(73407),c=t(2784),o=t(9108),l=t(35232),a=t(16336),d=t(36853),u=t(39013),x=t(29185),y=t(83613);let h=(0,c.memo)(()=>{let{daemons:e}=(0,c.useContext)(y.Z);return e?(0,r.jsx)(x.a,{content:e.content,position:"bottom",modifiers:{offset:{enabled:!0,options:{offset:[0,28]}}},children:(0,r.jsx)(d.JO,{name:"warning",color:u.qr()})}):null});var m=t(35002),p=t(79106);let j=(0,c.memo)(e=>{let{placeholder:n}=e,{codeLocations:t}=(0,c.useContext)(y.Z);return t?"spinner"===t.type?(0,r.jsx)(m.u,{content:t.content,placement:"bottom",children:(0,r.jsx)(p.$,{purpose:"body-text",fillColor:u.md()})}):(0,r.jsx)(x.a,{content:t.content,position:"bottom",modifiers:{offset:{enabled:!0,options:{offset:[0,28]}}},children:(0,r.jsx)(d.JO,{name:"warning",color:u.qr()})}):n?(0,r.jsx)("div",{style:{width:"16px"}}):null});var g=t(89918);let f=e=>{let{refreshState:n,tab:t}=e,{healthTitle:d}=(0,c.useContext)(o.N),u=(0,l._)();return(0,r.jsxs)(i.x,{flex:{direction:"row",justifyContent:"space-between",alignItems:"flex-end"},children:[(0,r.jsxs)(s.mQ,{selectedTabId:t,children:[(0,r.jsx)(g.f,{id:"locations",title:"Code locations",to:"/locations",icon:(0,r.jsx)(j,{placeholder:!1})}),(0,r.jsx)(g.f,{id:"health",title:d,to:"/health",icon:(0,r.jsx)(h,{})}),u?(0,r.jsx)(g.f,{id:"concurrency",title:"Concurrency limits",to:"/concurrency"}):null,u?(0,r.jsx)(g.f,{id:"config",title:"Configuration",to:"/config"}):null]}),n?(0,r.jsx)(i.x,{padding:{bottom:8},children:(0,r.jsx)(a.xi,{refreshState:n})}):null]})}},35232:function(e,n,t){t.d(n,{_:function(){return i}});var r=t(20958);let i=()=>{let e=(0,r.aM)(s);return!!e.data?.instance.hasInfo},s=(0,r.Ps)`
  query InstanceConfigHasInfo {
    instance {
      id
      hasInfo
    }
  }
`}}]);
//# sourceMappingURL=6023.cc7027ab08603f1f.js.map