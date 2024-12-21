"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[4787],{74787:function(e,t,n){n.r(t),n.d(t,{InstanceHealthPage:function(){return g},InstanceHealthPageContent:function(){return p}});var o=n(52322),s=n(14934),i=n(39013),l=n(35292),a=n(74580),r=n(2784),c=n(30623),d=n(40782),u=n(9108),x=n(77265),f=n(20958),h=n(16336),j=n(69737),m=n(20171);let p=()=>{(0,j.Px)(),(0,m.j)("Daemons");let e=(0,f.aM)(C,{notifyOnNetworkStatusChange:!0}),t=(0,h.C4)(e,h.dT),{loading:n,data:a}=e;return(0,o.jsxs)("div",{style:{overflowY:"auto"},children:[(0,o.jsxs)(s.x,{padding:{vertical:16,horizontal:24},flex:{direction:"row",alignItems:"center",justifyContent:"space-between"},children:[(0,o.jsx)(l.pm,{children:"Daemon statuses"}),(0,o.jsx)("div",{children:(0,o.jsx)(h.xi,{refreshState:t})})]}),n&&!a?.instance?(0,o.jsx)(s.x,{padding:{horizontal:24},style:{color:i.$()},children:"Loading…"}):a?.instance?(0,o.jsx)(c.W,{daemonStatuses:a.instance.daemonHealth.allDaemonStatuses}):null]})},g=()=>{let{pageTitle:e}=(0,r.useContext)(u.N);return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(a.m,{title:(0,o.jsx)(l.X6,{children:e}),tabs:(0,o.jsx)(x.Z,{tab:"health"})}),(0,o.jsx)(p,{})]})};t.default=g;let C=(0,f.Ps)`
  query InstanceHealthQuery {
    instance {
      id
      ...InstanceHealthFragment
    }
  }

  ${d.O}
`},77265:function(e,t,n){n.d(t,{Z:function(){return C}});var o=n(52322),s=n(14934),i=n(73407),l=n(2784),a=n(9108),r=n(35232),c=n(16336),d=n(36853),u=n(39013),x=n(29185),f=n(83613);let h=(0,l.memo)(()=>{let{daemons:e}=(0,l.useContext)(f.Z);return e?(0,o.jsx)(x.a,{content:e.content,position:"bottom",modifiers:{offset:{enabled:!0,options:{offset:[0,28]}}},children:(0,o.jsx)(d.JO,{name:"warning",color:u.qr()})}):null});var j=n(35002),m=n(79106);let p=(0,l.memo)(e=>{let{placeholder:t}=e,{codeLocations:n}=(0,l.useContext)(f.Z);return n?"spinner"===n.type?(0,o.jsx)(j.u,{content:n.content,placement:"bottom",children:(0,o.jsx)(m.$,{purpose:"body-text",fillColor:u.md()})}):(0,o.jsx)(x.a,{content:n.content,position:"bottom",modifiers:{offset:{enabled:!0,options:{offset:[0,28]}}},children:(0,o.jsx)(d.JO,{name:"warning",color:u.qr()})}):t?(0,o.jsx)("div",{style:{width:"16px"}}):null});var g=n(89918);let C=e=>{let{refreshState:t,tab:n}=e,{healthTitle:d}=(0,l.useContext)(a.N),u=(0,r._)();return(0,o.jsxs)(s.x,{flex:{direction:"row",justifyContent:"space-between",alignItems:"flex-end"},children:[(0,o.jsxs)(i.mQ,{selectedTabId:n,children:[(0,o.jsx)(g.f,{id:"locations",title:"Code locations",to:"/locations",icon:(0,o.jsx)(p,{placeholder:!1})}),(0,o.jsx)(g.f,{id:"health",title:d,to:"/health",icon:(0,o.jsx)(h,{})}),u?(0,o.jsx)(g.f,{id:"concurrency",title:"Concurrency limits",to:"/concurrency"}):null,u?(0,o.jsx)(g.f,{id:"config",title:"Configuration",to:"/config"}):null]}),t?(0,o.jsx)(s.x,{padding:{bottom:8},children:(0,o.jsx)(c.xi,{refreshState:t})}):null]})}},35232:function(e,t,n){n.d(t,{_:function(){return s}});var o=n(20958);let s=()=>{let e=(0,o.aM)(i);return!!e.data?.instance.hasInfo},i=(0,o.Ps)`
  query InstanceConfigHasInfo {
    instance {
      id
      hasInfo
    }
  }
`}}]);
//# sourceMappingURL=4787.adb4179a77eae728.js.map