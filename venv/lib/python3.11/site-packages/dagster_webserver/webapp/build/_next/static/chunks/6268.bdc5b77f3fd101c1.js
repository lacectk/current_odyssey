"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[6268],{35232:function(e,t,n){n.d(t,{_:function(){return s}});var r=n(20958);let s=()=>{let e=(0,r.aM)(l);return!!e.data?.instance.hasInfo},l=(0,r.Ps)`
  query InstanceConfigHasInfo {
    instance {
      id
      hasInfo
    }
  }
`},6268:function(e,t,n){n.r(t),n.d(t,{RunsRoot:function(){return V},default:function(){return M}});var r=n(52322),s=n(49853),l=n(14934),i=n(49308),a=n(74580),u=n(35292),o=n(37483),d=n(89891),c=n(80122),h=n(96795),f=n.n(h),x=n(2784),p=n(16629),m=n(47933),j=n(20958),g=n(9108),y=n(35232);let C=()=>{let e=(0,y._)();return(0,r.jsxs)(l.x,{flex:{direction:"column",gap:8},style:{minWidth:"100%"},border:"bottom",children:[e&&(0,r.jsx)(p.b,{intent:"info",title:(0,r.jsx)(m.rU,{to:"/config#run_coordinator",children:"View queue configuration"})}),e&&(0,r.jsx)(R,{})]})},R=()=>{let{data:e}=(0,j.aM)(v),{pageTitle:t}=(0,x.useContext)(g.N),n=e?.instance.daemonHealth.daemonStatus;return n?.required&&!n?.healthy?(0,r.jsx)(p.b,{intent:"warning",title:"The queued run coordinator is not healthy.",description:(0,r.jsxs)("div",{children:["View ",(0,r.jsx)(m.rU,{to:"/health",children:t})," for details."]})}):null},v=(0,j.Ps)`
  query QueueDaemonStatusQuery {
    instance {
      id
      daemonHealth {
        id
        daemonStatus(daemonType: "QUEUED_RUN_COORDINATOR") {
          id
          daemonType
          healthy
          required
        }
      }
    }
  }
`;var w=n(38267),E=n(59351),b=n(71902),k=n(33182),_=n(52338),q=n(38491),T=n(18149),A=n.n(T),I=n(96569);let P=e=>{let{refetch:t,filter:n,disabled:s}=e,[l,i]=(0,x.useState)(null),a=(0,j.xJ)(),u=async()=>{let e=await a.query({query:N,variables:{filter:n}});i("Runs"===e.data.pipelineRunsOrError.__typename?Object.fromEntries(e.data.pipelineRunsOrError.results.map(e=>[e.id,e.canTerminate])):{})};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(I.A,{isOpen:null!==l,selectedRuns:l||{},selectedRunsAllQueued:A()(n,{statuses:Array.from(E.sy)}),onClose:()=>i(null),onComplete:()=>t()}),(0,r.jsx)(q.zx,{intent:"danger",outlined:!0,disabled:s,onClick:u,children:"Terminate all…"})]})},N=(0,j.Ps)`
  query TerminateRunIdsQuery($filter: RunsFilter!) {
    pipelineRunsOrError(filter: $filter) {
      ... on Runs {
        results {
          id
          status
          canTerminate
        }
      }
    }
  }
`;var O=n(74928),Q=n(16336),S=n(69737),H=n(28316);function U(e){let{node:t}=e,n=x.useRef(null);return x.useLayoutEffect(()=>{let e=n.current;if(t.parentNode!==e){if(null!==t.parentNode&&t.parentNode!==e)throw Error("Cannot render the same node twice");return e?.appendChild(t),()=>{e?.removeChild(t)}}},[t]),(0,r.jsx)("div",{ref:n})}var D=n(59923),F=n(85062);let V=()=>{(0,S.Px)();let[e,t]=(0,_.oD)(),n=(0,_.VH)(e),{queryResult:h,paginationProps:p}=(0,O.E)(n,25),m=(0,Q.C4)(h,Q.dT),j=(0,w.Qf)(e),g="all"!==j,[y,R]=f()(e,e=>"status"===e.token),v=(0,x.useCallback)(e=>{g?t([...y,...e]):t(e)},[t,g,y]),q=(0,x.useCallback)(e=>{let t=(0,s.HY)(e);R.some(e=>(0,s.HY)(e)===t)||v([...R,e])},[R,v]),T=(0,x.useMemo)(()=>{let e=["tag","snapshotId","id","job","pipeline","partition","backfill"];return g||e.push("status"),e},[g]),A=(0,x.useMemo)(()=>g?e.filter(e=>"status"!==e.token):e,[e,g]),{tabs:I,queryResult:N}=(0,w.zc)(n),V=(0,Q.C4)(N,Q.dT),M=(0,Q.V5)(V,m),{button:Y,activeFiltersJsx:z}=(0,_.Vv)({tokens:A,onChange:v,enabledFilters:T}),[L,$]=function(e){let[t,n]=x.useState(null);return x.useLayoutEffect(()=>{n(e=>null===e?document.createElement("div"):e)},[]),[t?(0,H.createPortal)(e,t):null,t?(0,r.jsx)(U,{node:t}):null]}(Y);function B(){return(0,r.jsxs)(l.x,{style:{width:"100%",marginRight:8},flex:{justifyContent:"space-between"},children:[(0,r.jsxs)(l.x,{flex:{direction:"row",alignItems:"center",gap:8},children:[I,$]}),"queued"===j?(0,r.jsx)(P,{refetch:M.refetch,filter:{...n,statuses:Array.from(E.sy)},disabled:N.data?.queuedCount.__typename!=="Runs"||N.data?.queuedCount.count===0}):"in-progress"===j?(0,r.jsx)(P,{refetch:M.refetch,filter:{...n,statuses:Array.from(E.Ys)},disabled:N.data?.inProgressCount.__typename!=="Runs"||N.data?.inProgressCount.count===0}):void 0]})}return(0,r.jsxs)(i.T,{children:[(0,r.jsx)(a.m,{title:(0,r.jsx)(u.X6,{children:"Runs"}),right:(0,r.jsx)(Q.xi,{refreshState:M})}),L,(0,r.jsx)(k.Lw.Provider,{value:{refetch:h.refetch},children:(0,r.jsx)(D.g,{queryResult:h,allowStaleData:!0,renderError:e=>{let t=!!(e?.networkError&&"statusCode"in e.networkError&&400===e.networkError.statusCode);return(0,r.jsxs)(l.x,{flex:{direction:"column",gap:32},padding:{vertical:8,horizontal:24},children:[B(),(0,r.jsx)(o.t,{icon:"warning",title:t?"Invalid run filters":"Unexpected error",description:t?"The specified run filters are not valid. Please check the filters and try again.":"An unexpected error occurred. Check the console for details."})]})},children:e=>{let{pipelineRunsOrError:t}=e;return"Runs"!==t.__typename?(0,r.jsx)(l.x,{padding:{vertical:64},children:(0,r.jsx)(o.t,{icon:"error",title:"Query Error",description:t.message})}):(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(F.Y,{$top:0,children:(0,r.jsx)(b.A,{runs:t.results,onAddTag:q,filter:n,actionBarComponents:B(),belowActionBarComponents:"queued"===j||z.length?(0,r.jsxs)(r.Fragment,{children:["queued"===j&&(0,r.jsx)(C,{}),z.length>0&&(0,r.jsxs)(r.Fragment,{children:[z,(0,r.jsx)(d.Z,{onClick:()=>v([]),children:"Clear all"})]})]}):null})}),t.results.length>0?(0,r.jsx)("div",{style:{marginTop:"16px"},children:(0,r.jsx)(c.pI,{...p})}):null]})}})})]})};var M=V}}]);
//# sourceMappingURL=6268.bdc5b77f3fd101c1.js.map